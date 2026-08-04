'use strict';

const crypto = require('crypto');
const { StringDecoder } = require('string_decoder');
const markers = require('./ui-markers');
const {
  responsesInputToChatMessages,
  responsesToolsToChatTools,
} = require('../adaptor/completion-api-adaptor');

// dragonai-brain.js
//
// Brain mode: a pure wire-protocol bridge between the Codex Responses API and
// the dragonai-agent/v1 protocol served by the DragonAI Brain
// (POST /agent/v1/turn). When DRAGONAI_BRAIN_URL is set, proxy.js hands every
// POST /responses turn to runBrainTurn instead of forwarding to the static
// upstream. This module only translates shapes in both directions:
//
//   Responses request  -> CODEX_MODEL_REQUEST envelope (buildModelRequest)
//   MODEL_DELTA        -> response.output_text.delta
//   TOOL_REQUEST       -> function_call output items (through the proxy's
//                         translateOutputItem so namespace/custom tools are
//                         restored, same as the normal upstream path)
//   PLAN_UPDATE /      -> small completed-message UI markers (best effort;
//   TOOL_RESULT           never allowed to break the stream)
//   TOOL_DECISION /    -> observability frames, tolerated and ignored — as is
//   BRAIN_REASONING_REQUEST  every unknown event type
//   MODEL_RESULT       -> response.completed / final Responses JSON
//
// All routing, model selection, and tool policy live in the Brain; the proxy
// holds no intelligence beyond this translation.

const PROTOCOL = 'dragonai-agent/v1';

function brainUrl(env = process.env) {
  const url = env.DRAGONAI_BRAIN_URL || '';
  return String(url).trim().replace(/\/+$/u, '');
}

function enabled(env = process.env) {
  return brainUrl(env) !== '';
}

function brainApiKey(env = process.env) {
  return env.DRAGONAI_BRAIN_API_KEY || '';
}

function now() {
  return Math.floor(Date.now() / 1000);
}

function genId(prefix) {
  return prefix + '_' + crypto.randomBytes(12).toString('hex');
}

// SSE keepalive interval towards Codex. Codex's Responses reader arms its
// idle timer around `stream.next()` on a parsed SSE *event*
// (codex-rs/codex-api/src/sse/responses.rs `timeout(idle_timeout,
// stream.next())`), so comment lines (`: ping`) never reset it. While the
// Brain thinks (a local worker inference can stay silent for minutes) we must
// emit a real event or Codex fails the turn with "idle timeout waiting for
// SSE". 0 disables the heartbeat.
const SSE_HEARTBEAT_DEFAULT_MS = 15000;

function sseHeartbeatMs(env = process.env) {
  const raw = env.DRAGONAI_SSE_HEARTBEAT_MS;
  if (raw === undefined || String(raw).trim() === '') return SSE_HEARTBEAT_DEFAULT_MS;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) return SSE_HEARTBEAT_DEFAULT_MS;
  return Math.floor(value);
}

// Plain text of a Responses message content value (string or part array).
function contentText(content) {
  if (content == null) return '';
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  const pieces = [];
  for (const part of content) {
    if (typeof part === 'string') pieces.push(part);
    else if (part && typeof part === 'object' && typeof part.text === 'string') pieces.push(part.text);
  }
  return pieces.filter(Boolean).join('\n');
}

function firstUserText(body) {
  const input = body && body.input;
  if (typeof input === 'string') return input;
  if (!Array.isArray(input)) return '';
  for (const item of input) {
    if (!item || typeof item !== 'object') continue;
    const role = item.role || (item.type === 'message' ? 'user' : null);
    if (role === 'user') return contentText(item.content);
  }
  return '';
}

// Stable per-session id: the Responses wire protocol carries no session id, so
// derive one from the parts that stay constant across turns of one Codex
// session (see the dragonai-agent/v1 spec).
function headerValue(headers, name) {
  if (!headers) return '';
  const wanted = name.toLowerCase();
  if (typeof headers.get === 'function') return String(headers.get(name) || '');
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === wanted) return Array.isArray(value) ? String(value[0] || '') : String(value || '');
  }
  return '';
}

function transcriptHash(body) {
  const value = JSON.stringify((body && body.input) || []);
  return crypto.createHash('sha256').update(value).digest('hex');
}

function codexTurnMetadata(headers) {
  const raw = headerValue(headers, 'x-codex-turn-metadata');
  if (!raw || raw.length > 128 * 1024) return {};
  try {
    const value = JSON.parse(raw);
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  } catch {
    return {};
  }
}

function workspaceFromTurnMetadata(metadata) {
  for (const candidate of [metadata && metadata.cwd, metadata && metadata.repo_root]) {
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
  }
  const workspaces = metadata && metadata.workspaces &&
    typeof metadata.workspaces === 'object' && !Array.isArray(metadata.workspaces)
    ? metadata.workspaces : {};
  const entries = Object.entries(workspaces).filter(([path]) => Boolean(String(path || '').trim()));
  if (!entries.length) return '';
  const changed = entries.find(([, value]) => value && value.has_changes === true);
  return String((changed || entries[0])[0]);
}

function workspaceFromInstructions(instructions) {
  const match = String(instructions || '').match(/<cwd>\s*([^<\r\n]+?)\s*<\/cwd>/iu);
  return match ? match[1].trim() : '';
}

function conversationId(body, metadata = {}) {
  const explicit = metadata.conversation_id || metadata.thread_id || '';
  if (explicit) return String(explicit);
  const instructions = body && body.instructions ? String(body.instructions) : '';
  const anchor = instructions + '\0' + firstUserText(body);
  return 'codex-' + crypto.createHash('sha256').update(anchor).digest('hex').slice(0, 24);
}

// Responses request (already run through proxy.js translateRequestBody: flat
// function tools, message / function_call / function_call_output input items)
// -> dragonai-agent/v1 CODEX_MODEL_REQUEST envelope.
function buildModelRequest(body, opts = {}) {
  const src = body && typeof body === 'object' ? body : {};
  const stream = opts.stream !== undefined ? Boolean(opts.stream) : src.stream === true;
  // Instructions travel in payload.instructions; strip them from the message
  // conversion so they are not duplicated as a system message.
  const withoutInstructions = Object.assign({}, src, { instructions: undefined });
  const headers = opts.headers || {};
  const native = codexTurnMetadata(headers);
  const nativeThreadId = headerValue(headers, 'thread-id')
    || headerValue(headers, 'x-client-request-id')
    || String(native.thread_id || '');
  const explicitConversation = nativeThreadId
    || headerValue(headers, 'x-dragonai-conversation-id')
    || headerValue(headers, 'x-codex-thread-id');
  const backendSessionId = headerValue(headers, 'session-id')
    || String(native.session_id || '')
    || headerValue(headers, 'x-dragonai-backend-session-id');
  const nativeTurnId = String(native.turn_id || '');
  const taskId = nativeTurnId
    || headerValue(headers, 'x-dragonai-task-id')
    || backendSessionId;
  const forkedFrom = String(native.forked_from_thread_id || '');
  const parentConversationId = forkedFrom
    || headerValue(headers, 'x-codex-parent-thread-id')
    || String(native.parent_thread_id || '')
    || headerValue(headers, 'x-dragonai-parent-conversation-id');
  const subagent = headerValue(headers, 'x-openai-subagent')
    || String(native.subagent_kind || native.subagent || '');
  const operation = forkedFrom
    ? 'fork'
    : (subagent === 'compact'
      ? 'compact'
      : (headerValue(headers, 'x-dragonai-conversation-operation') || 'continue'));
  const metadata = {
    backend: headerValue(headers, 'x-dragonai-backend') || 'codex',
    backend_session_id: backendSessionId,
    task_id: taskId,
    thread_id: nativeThreadId || headerValue(headers, 'x-codex-thread-id'),
    parent_conversation_id: parentConversationId,
    operation,
    route_preference: headerValue(headers, 'x-dragonai-route-preference'),
    capability: headerValue(headers, 'x-dragonai-capability'),
    transcript_hash: transcriptHash(src),
    workspace: workspaceFromTurnMetadata(native) ||
      workspaceFromInstructions(src.instructions),
  };
  return {
    protocol: PROTOCOL,
    event: 'CODEX_MODEL_REQUEST',
    turn_id: opts.turnId || 't-' + crypto.randomBytes(4).toString('hex'),
    conversation_id: conversationId(src, { conversation_id: explicitConversation }),
    payload: {
      model_hint: src.model || 'dragonai/auto',
      instructions: src.instructions ? String(src.instructions) : '',
      messages: responsesInputToChatMessages(withoutInstructions),
      tools: responsesToolsToChatTools(src.tools),
      // Wire-kind hints ([{name, wire, namespace, base}]) preserving the
      // original Responses tool types the flat function list drops
      // (additive; the Brain tolerates their absence).
      tool_registry: Array.isArray(opts.toolRegistry) ? opts.toolRegistry : [],
      tool_choice: src.tool_choice || 'auto',
      parallel_tool_calls: src.parallel_tool_calls !== false,
      stream,
      metadata,
    },
  };
}

function mapUsage(usage) {
  if (!usage || typeof usage !== 'object') return null;
  const input = Number(usage.input_tokens) || 0;
  const output = Number(usage.output_tokens) || 0;
  return { input_tokens: input, output_tokens: output, total_tokens: input + output };
}

function messageItem(id, text) {
  return {
    id,
    type: 'message',
    status: 'completed',
    role: 'assistant',
    content: [{ type: 'output_text', text, annotations: [] }],
  };
}

// TOOL_REQUEST frame / MODEL_RESULT tool_requests entry -> Responses
// function_call item. call_id is preserved so Codex can pair the eventual
// function_call_output.
function functionCallItem(toolReq) {
  return {
    id: toolReq.id || genId('fc'),
    type: 'function_call',
    status: 'completed',
    call_id: toolReq.call_id || toolReq.id || genId('call'),
    name: toolReq.name || 'unknown_tool',
    arguments: typeof toolReq.arguments === 'string' ? toolReq.arguments : JSON.stringify(toolReq.arguments || {}),
  };
}

// Run an item through the proxy's translateOutputItem (restores namespaced and
// custom tools). Keep the original on null/DROP/throw so a translation problem
// can never lose the tool call.
function applyTranslate(item, translateOutputItem) {
  if (typeof translateOutputItem !== 'function') return item;
  try {
    const out = translateOutputItem(item);
    if (out == null || typeof out === 'symbol') return item;
    return out;
  } catch {
    return item;
  }
}

function parseSseBlock(block) {
  let event = '';
  const data = [];
  for (const line of block.split(/\r?\n/u)) {
    if (line.startsWith('event:')) event = line.slice(6).trim();
    else if (line.startsWith('data:')) data.push(line.slice(5).trimStart());
  }
  return { event, data: data.join('\n') };
}

function sendError(res, message) {
  if (!res.headersSent) {
    const payload = JSON.stringify({ error: { message } });
    res.writeHead(502, {
      'content-type': 'application/json',
      'content-length': Buffer.byteLength(payload),
    });
    res.end(payload);
  } else {
    res.end();
  }
}

function truncate(text, max = 300) {
  const s = String(text);
  return s.length > max ? s.slice(0, max) + '…' : s;
}

function friendlyTool(item) {
  if (!item || typeof item !== 'object') return '';
  const name = String(item.name || 'unknown');
  const source = String(item.source || '');
  const short = name.split('__').filter(Boolean).at(-1) || name;
  if (source.startsWith('mcp:')) {
    return 'MCP ' + source.slice(4) + ':' + short;
  }
  if (source === 'codex:collaboration') return 'Codex core:' + short;
  if (source.startsWith('browser/')) return 'Browser:' + short;
  return 'Codex:' + short;
}

function tableCell(value) {
  return String(value || '').replace(/\|/gu, '\\|').replace(/\r?\n/gu, ' ');
}

function selectedToolsTable(selected) {
  if (!Array.isArray(selected) || !selected.length) return [];
  const lines = [
    '',
    '| # | Runtime | Selected tool |',
    '|---:|---|---|',
  ];
  selected.forEach((item, index) => {
    const friendly = friendlyTool(item);
    const separator = friendly.indexOf(':');
    const runtime = separator >= 0 ? friendly.slice(0, separator) : 'Codex';
    const tool = separator >= 0 ? friendly.slice(separator + 1) : friendly;
    lines.push(
      '| ' + (index + 1) +
      ' | ' + tableCell(runtime) +
      ' | `' + tableCell(tool) + '` |'
    );
  });
  return lines;
}

// PLAN_UPDATE `plan` payload -> headline + Markdown step table (Plan Mode).
// Python only sends structured rows; the Markdown is assembled here, same
// escaping pattern as selectedToolsTable.
function planTableText(plan) {
  if (!plan || typeof plan !== 'object') return '';
  const rows = Array.isArray(plan.table_rows) ? plan.table_rows : [];
  if (!rows.length) return '';
  const done = rows.filter((row) => row && row.status === 'completed').length;
  const lines = [
    '◈ DragonAI Plan · ' + rows.length + ' steps (' + done + ' done)' +
    (plan.planner_model ? ' · planner=' + plan.planner_model : '') +
    ' · codex update_plan=' + (plan.codex_plan || 'pending'),
  ];
  if (plan.codex_plan_error) {
    lines.push('codex says: ' + truncate(plan.codex_plan_error, 300));
  }
  lines.push(
    '',
    '| # | St | Step | Tool | Owner | Try | Note |',
    '|---:|:--:|---|---|---|---:|---|'
  );
  rows.forEach((row) => {
    if (!row || typeof row !== 'object') return;
    lines.push(
      '| ' + (Number(row.n) || 0) +
      ' | ' + tableCell(row.glyph || '□') +
      ' | ' + tableCell(truncate(row.step || '', 160)) +
      ' | ' + (row.tool ? '`' + tableCell(row.tool) + '`' : '-') +
      ' | ' + tableCell(row.owner || 'codex') +
      ' | ' + (Number(row.attempts) || 0) +
      ' | ' + tableCell(truncate(row.note || '', 160)) + ' |'
    );
  });
  return lines.join('\n');
}

// PLAN_UPDATE `planner` block (P4) -> one-line planner ladder chip:
//   planner ladder: 14b(parse_failed) → 30b(ok) · complexity=0.72
//   · floor=worker-30b · confidence=0.61 (planner)
// A single successful attempt with no escalation renders as one segment.
function plannerLadderText(planner) {
  if (!planner || typeof planner !== 'object') return '';
  try {
    const attempts = Array.isArray(planner.attempts) ? planner.attempts : [];
    if (!attempts.length) return '';
    const tierNames = {
      'router-14b': '14b',
      'worker-30b': '30b',
      remote: 'remote',
      deterministic: 'deterministic',
    };
    const chain = attempts
      .map((attempt) => {
        if (!attempt || typeof attempt !== 'object') return '';
        const tier = tierNames[attempt.tier] || String(attempt.tier || '?');
        const status = attempt.ok ? 'ok' : (attempt.reason || 'failed');
        return tier + '(' + truncate(String(status), 40) + ')';
      })
      .filter(Boolean)
      .join(' → ');
    if (!chain) return '';
    const bits = ['planner ladder: ' + chain];
    const complexity = planner.complexity && typeof planner.complexity === 'object'
      ? planner.complexity : {};
    if (Number.isFinite(Number(complexity.score))) {
      bits.push('complexity=' + Number(complexity.score).toFixed(2));
    }
    if (complexity.tier_floor) {
      bits.push('floor=' + truncate(String(complexity.tier_floor), 40));
    }
    if (Number.isFinite(Number(planner.confidence))) {
      bits.push(
        'confidence=' + Number(planner.confidence).toFixed(2) +
        (planner.confidence_source
          ? ' (' + truncate(String(planner.confidence_source), 40) + ')'
          : '')
      );
    }
    return bits.join(' · ');
  } catch {
    return '';
  }
}

// MEMORY_RECALL frame (P5) -> one-line layered-memory chip:
//   ◆ Memory recall · 2 playbook(s) · 5 fact(s) · 1 lesson(s)
//   · top="edit: fix retry" (win 4/5)
function memoryRecallText(memory) {
  if (!memory || typeof memory !== 'object') return '';
  try {
    const playbooks = Number(memory.playbooks) || 0;
    const facts = Number(memory.facts) || 0;
    const lessons = Number(memory.lessons) || 0;
    if (!playbooks && !facts && !lessons) return '';
    const bits = ['◆ Memory recall'];
    if (playbooks) bits.push(playbooks + ' playbook(s)');
    if (facts) bits.push(facts + ' fact(s)');
    if (lessons) bits.push(lessons + ' lesson(s)');
    const top = memory.top_playbook && typeof memory.top_playbook === 'object'
      ? memory.top_playbook : null;
    if (top && (top.title || top.playbook_id)) {
      const wins = Number(top.wins) || 0;
      const losses = Number(top.losses) || 0;
      bits.push(
        'top="' + truncate(String(top.title || top.playbook_id), 80) + '"' +
        ' (win ' + wins + '/' + (wins + losses) + ')'
      );
    }
    return bits.join(' · ');
  } catch {
    return '';
  }
}

// PLAN_STEP_UPDATE frame -> a one-line step progress chip.
function stepUpdateText(data) {
  if (!data || typeof data !== 'object') return '';
  const step = data.step && typeof data.step === 'object' ? data.step : null;
  if (!step) return '';
  const total = Number(data.total) || 0;
  const position = 'Step ' + (Number(step.n) || '?') + '/' + (total || '?');
  const phase = String(data.phase || '');
  if (phase === 'delegated' || step.status === 'in_progress') {
    return '▶ ' + position +
      (step.tool ? ' · ' + step.tool : '') +
      ' · attempt ' + (Number(step.attempts) || 1);
  }
  if (phase === 'verified' || step.status === 'completed') {
    return '✔ ' + position + ' done';
  }
  if (phase === 'rejected' || phase === 'retry' || step.status === 'blocked') {
    return '✗ ' + position + ' failed' +
      (step.note ? ': ' + truncate(step.note, 300) : '');
  }
  return '◇ ' + position + ' · ' + (step.status || 'pending');
}

function planMarkerText(data) {
  if (!data || typeof data !== 'object') return '';
  const lane = data.lane || '';
  const model = data.model || '';
  if (!lane && !model) return '';
  const stageIntent = data.stage_intent && typeof data.stage_intent === 'object'
    ? data.stage_intent : null;
  const stageFailure = data.stage_failure && typeof data.stage_failure === 'object'
    ? data.stage_failure : null;
  if (stageFailure) {
    const lines = [
      '✗ Codex staging transaction · ' + (stageFailure.status || 'failed'),
      'why: ' + truncate(stageFailure.reason || 'No verified tool result', 700),
    ];
    if (stageFailure.manual_command) {
      lines.push('failed/manual command: ' + truncate(stageFailure.manual_command, 1000));
    }
    if (stageFailure.fallback_instruction) {
      lines.push('DragonAI fallback: reply `' + truncate(stageFailure.fallback_instruction, 200) + '`');
    }
    if (stageFailure.model_output) {
      lines.push('model output: ' + truncate(stageFailure.model_output, 700));
    }
    return lines.join('\n');
  }
  if (stageIntent) {
    const selected = Array.isArray(stageIntent.selected_paths)
      ? stageIntent.selected_paths : [];
    const mixed = Array.isArray(stageIntent.mixed_paths)
      ? stageIntent.mixed_paths : [];
    // Phase-aware heading (D9). Events without a phase keep the legacy
    // wording so older Brain builds render unchanged.
    const stagePhaseHeadings = {
      intent: '◇ staging intent · awaiting Codex execution',
      delegated: '▶ staging delegated to Codex',
      verified: '✔ staging verified',
      rejected: '✗ staging rejected',
    };
    const phase = typeof stageIntent.phase === 'string' ? stageIntent.phase : '';
    const heading = stagePhaseHeadings[phase]
      || ('◇ LLM staging intent · ' + (stageIntent.status || 'unknown'));
    const lines = [
      heading,
      'source: ' + (stageIntent.source || 'model'),
    ];
    if (phase && stageIntent.status) {
      lines.push('status: ' + truncate(String(stageIntent.status), 120));
    }
    if (stageIntent.retry) {
      lines.push('retry: ' + truncate(String(stageIntent.retry), 60));
    }
    if (selected.length) {
      lines.push('wants to stage: ' + truncate(selected.join(', '), 700));
    }
    if (mixed.length) {
      lines.push('mixed-scope, not staged whole: ' + truncate(mixed.join(', '), 700));
    }
    if (stageIntent.command) {
      lines.push('Codex transaction: ' + truncate(stageIntent.command, 900));
    }
    if (stageIntent.reason) {
      lines.push('why: ' + truncate(stageIntent.reason, 500));
    }
    if (stageIntent.workspace) {
      lines.push('workspace: ' + truncate(stageIntent.workspace, 900));
    }
    if (!selected.length && stageIntent.raw_preview) {
      lines.push('LLM output: ' + truncate(stageIntent.raw_preview, 700));
    }
    return lines.join('\n');
  }
  const lines = [];
  const details = data.neural_route && typeof data.neural_route === 'object'
    ? data.neural_route : {};
  const plan = data.turn_plan && typeof data.turn_plan === 'object'
    ? data.turn_plan : {};
  const planner = data.planner && typeof data.planner === 'object'
    ? data.planner : {};
  const budget = data.context_budget && typeof data.context_budget === 'object'
    ? data.context_budget : {};
  const toolProfile = data.tool_profile && typeof data.tool_profile === 'object'
    ? data.tool_profile : {};
  const routeBits = [
    data.category ? 'task=' + data.category : '',
    details.final_action ? 'action=' + details.final_action : '',
  ].filter(Boolean);
  const routePrefix = data.is_continuation ? 'continue' : 'route';
  lines.push('◇ DragonAI Assistant');
  if (!data.is_continuation && plan.goal) {
    lines.push('◆ User goal · ' + truncate(plan.goal, 240));
  }
  lines.push(
    routePrefix + ': ' + lane + '/' + model +
    (routeBits.length ? ' · ' + routeBits.join(' · ') : '')
  );
  if (!data.is_continuation && data.reason) {
    lines.push('why: ' + truncate(data.reason, 240));
  }
  if (!data.is_continuation && planner.actual_model) {
    lines.push(
      'planner: ' + planner.actual_model +
      ' · recommends=' + (planner.recommendation || 'unspecified') +
      ' · task=' + (planner.task_family || data.category || 'unknown') +
      ' · risk=' + (planner.risk || data.risk || 'unknown') +
      ' · confidence=' + (
        Number.isFinite(Number(planner.confidence))
          ? Number(planner.confidence).toFixed(2)
          : '-'
      ) +
      (planner.confidence_source
        ? ' (' + truncate(String(planner.confidence_source), 40) + ')'
        : '')
    );
  }
  const probabilities = details.probabilities &&
    typeof details.probabilities === 'object' ? details.probabilities : {};
  const probabilityParts = [
    ['local', probabilities.local_direct],
    ['probe', probabilities.local_probe],
    ['high', probabilities.high_direct],
    ['evidence', probabilities.gather_evidence],
  ].filter((item) => Number.isFinite(Number(item[1])))
    .map((item) => item[0] + '=' + Number(item[1]).toFixed(2));
  if (data.is_continuation && planner.actual_model) {
    lines.push(
      'decision chain: planner=' + planner.actual_model +
      ' recommends ' + (planner.recommendation || 'unspecified') +
      ' · neural=' + (details.action || 'fallback') +
      ' · policy=' + (details.policy || 'adaptive') +
      ' · final=' + (
        details.final_action || details.requested_action || details.action || '-'
      ) +
      (probabilityParts.length
        ? ' · probabilities ' + probabilityParts.join('/')
        : '')
    );
  }
  if (!data.is_continuation && probabilityParts.length) {
    lines.push(
      'neural router: ' + probabilityParts.join(' · ') +
      (details.mode ? ' · mode=' + details.mode : '') +
      (details.bundle_version ? ' · bundle=' + details.bundle_version : '')
    );
  }
  if (!data.is_continuation && (
    details.action || details.requested_action || details.final_action
  )) {
    lines.push(
      'policy: ' + (details.policy || 'adaptive') +
      ' · neural=' + (details.action || 'fallback') +
      ' → policy=' + (details.requested_action || details.action || '-') +
      ' → final=' + (details.final_action || details.requested_action || '-')
    );
    if (details.lane_fallback) {
      lines.push('fallback: requested high lane is unavailable; using local worker');
    } else if (details.task_pin) {
      lines.push('route pin: kept ' + details.task_pin + ' for this tool transaction');
    }
  }
  if (details.configured_model && details.actual_model &&
      details.configured_model !== details.actual_model) {
    lines.push(
      'model resolved: ' + details.configured_model + ' → ' +
      details.actual_model + ' (' +
      (details.model_resolution || 'provider catalog') + ')'
    );
  }
  if (Array.isArray(plan.evidence_gaps) && plan.evidence_gaps.length) {
    lines.push(
      'evidence: ' + plan.evidence_gaps.length + ' gap(s)' +
      (!data.is_continuation
        ? ' · ' + truncate(plan.evidence_gaps.join('; '), 280)
        : '')
    );
  }
  if (Number.isFinite(Number(toolProfile.available_count))) {
    lines.push(
      'tools: ' + (toolProfile.selected_count || 0) +
      '/' + (toolProfile.available_count || 0) + ' selected · ' +
      (toolProfile.selected_tokens || 0) +
      '/' + (toolProfile.available_tokens || 0) + 't · profile=' +
      (toolProfile.profile || 'dynamic')
    );
    lines.push(...selectedToolsTable(toolProfile.selected));
  }
  if (budget.context_limit) {
    const before = budget.before || {};
    const after = budget.after || {};
    lines.push(
      'context: ' + (after.total_estimated_tokens || 0) +
      '/' + budget.input_budget +
      (Number(before.total_estimated_tokens || 0) !== Number(after.total_estimated_tokens || 0)
        ? ' · fitted from ' + (before.total_estimated_tokens || 0)
        : '') +
      ' · history=' + (after.history_tokens || 0) +
      ' · tools=' + (after.tool_schema_tokens || 0)
    );
    if (Array.isArray(budget.actions) && budget.actions.length) {
      lines.push('context fit: ' + truncate(budget.actions.join('; '), 700));
    }
  }
  return lines.join('\n');
}

// SUBAGENT_EXECUTION frame (Workstream S; also renders legacy
// FALLBACK_EXECUTION payloads) -> the ⬡ subagent marker.
//
// Direct-call phases (S-P2): offer / executing / done / failed
//   ⬡ OpenHands subagent · exec_readonly → done (812ms) · <preview>
// Delegated-task phases (S-P3, defined here from day one):
//   started / tool_call / observation / message / awaiting_approval /
//   finished — e.g. `⬡ OpenHands subagent · step 3 · terminal · pytest -x`
//   and `⬡ OpenHands subagent · ✔ 任务完成 · 17 steps · 4m12s`.
//
// runtime:"local" (and legacy payloads without a runtime field) keeps the
// historical fallback banner headline so the local tier still reads exactly
// like the P6 whitelist fallback:
//   ⚠ 未使用 Codex 提供的工具 — DragonAI 本地 fallback
function subagentDuration(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n < 0) return '';
  if (n < 60000) return (n >= 10000 ? Math.round(n / 1000) + 's' : n + 'ms');
  return Math.floor(n / 60000) + 'm' + String(Math.round((n % 60000) / 1000)).padStart(2, '0') + 's';
}

function subagentMarkerText(data) {
  if (!data || typeof data !== 'object') return '';
  try {
    const phase = String(data.phase || '');
    if (!phase) return '';
    // Legacy FALLBACK_EXECUTION payloads carry no runtime field: local.
    const local = String(data.runtime || 'local') === 'local';
    const head = local
      ? '⚠ 未使用 Codex 提供的工具 — DragonAI 本地 fallback'
      : '⬡ OpenHands subagent';
    const name = truncate(String(data.capability || data.tool || 'task'), 80);
    const duration = subagentDuration(data.duration_ms);
    const durationText = duration ? ' (' + duration + ')' : '';
    const rawPreview = data.output_preview || data.preview || '';
    const preview = rawPreview
      ? ' · ' + truncate(String(rawPreview).replace(/\s+/gu, ' '), 300)
      : '';

    if (phase === 'offer') {
      const lines = [head];
      if (!local) lines.push('⚠ 未使用 Codex 提供的工具');
      lines.push(
        'capability: ' + name +
        ' (' + (data.mutating ? 'mutating' : 'read-only') +
        (data.approval ? ', ' + truncate(String(data.approval), 160) : '') + ')'
      );
      if (data.reason) lines.push('why: ' + truncate(String(data.reason), 500));
      if (data.owner) lines.push('owner: ' + truncate(String(data.owner), 120));
      if (data.mutating && data.approval_phrase) {
        lines.push(
          '◇ offered · will NOT run until you reply exactly: `' +
          truncate(String(data.approval_phrase), 120) + '`'
        );
      } else {
        lines.push('◇ offered · read-only, executing now');
      }
      return lines.join('\n');
    }
    if (phase === 'executing') return head + ' · ' + name + ' → executing';
    if (phase === 'done') return head + ' · ' + name + ' → done' + durationText + preview;
    if (phase === 'failed') return head + ' · ' + name + ' → failed' + durationText + preview;

    // ---- delegated-task phases (S-P3 emits these) ----
    const step = Number.isFinite(Number(data.step)) && Number(data.step) > 0
      ? ' · step ' + Number(data.step)
      : '';
    const tool = data.tool ? ' · ' + truncate(String(data.tool), 60) : '';
    if (phase === 'started') {
      return head + ' · 任务开始' + preview + '\n⚠ 未使用 Codex 提供的工具';
    }
    if (phase === 'tool_call') return head + step + tool + preview;
    if (phase === 'observation') {
      const merged = Number(data.merged_count) > 1
        ? ' ×' + Number(data.merged_count)
        : '';
      return head + step + tool + ' ⇢ observation' + merged + preview;
    }
    if (phase === 'message') return head + step + preview;
    if (phase === 'awaiting_approval') {
      return head + step + ' · ⏸ 等待审批' +
        (data.risk ? ' · risk=' + truncate(String(data.risk), 20) : '') +
        preview;
    }
    if (phase === 'finished') {
      const failed = data.status && data.status !== 'finished' && data.status !== 'completed';
      const steps = Number.isFinite(Number(data.steps)) && Number(data.steps) > 0
        ? ' · ' + Number(data.steps) + ' steps'
        : '';
      return head + ' · ' + (failed ? '✗ 任务终止 (' + truncate(String(data.status), 40) + ')' : '✔ 任务完成') +
        steps + (duration ? ' · ' + duration : '') + preview;
    }
    return head + ' · phase: ' + truncate(phase, 40);
  } catch {
    return '';
  }
}

// Collab marker renderer (P7) -> the ⬢ collab marker. One function covers
// the whole collab event family: the v2 events (COLLAB_TASK_STARTED with
// version:"v2", COLLAB_EXPLORATION, COLLAB_REPORT, COLLAB_PLAN_REVIEW,
// COLLAB_DISPATCH, COLLAB_REVIEW_DECISION, COLLAB_STABLE_AUTO_OFFER,
// TOKEN_SAVINGS_ESTIMATED, COLLAB_TASK_COMPLETED, COLLAB_TASK_BLOCKED,
// COLLAB_PHASE_STARTED) and the v1 events kept alive alongside them
// (WORK_ORDER_CREATED, ROUND_COMPLETED, REVIEW_DECISION,
// LOCAL_ASSIST_REQUESTED, COLLAB_FALLBACK_REQUESTED). Unknown phases and
// missing fields never throw — a marker must never break the stream.
const COLLAB_MARKER_EVENTS = new Set([
  'COLLAB_TASK_STARTED',
  'COLLAB_EXPLORATION',
  'COLLAB_REPORT',
  'COLLAB_PLAN_REVIEW',
  'COLLAB_DISPATCH',
  'COLLAB_REVIEW_DECISION',
  'COLLAB_STABLE_AUTO_OFFER',
  'TOKEN_SAVINGS_ESTIMATED',
  'COLLAB_TASK_COMPLETED',
  'COLLAB_TASK_BLOCKED',
  'COLLAB_PHASE_STARTED',
  'WORK_ORDER_CREATED',
  'ROUND_COMPLETED',
  'REVIEW_DECISION',
  'LOCAL_ASSIST_REQUESTED',
  'COLLAB_FALLBACK_REQUESTED',
]);

// Human labels for the v2 phase machine (unknown phases render verbatim).
const COLLAB_PHASE_LABELS = {
  EXPLORING: '探索(本地, 只读)',
  REPORT_DRAFTING: '报告起草',
  REPORT_REVIEW: '报告审批',
  PRE_PLANNING: '预规划',
  PRE_PLAN_REVIEW: '计划审批',
  IMPLEMENTING: '实施',
  CORRECTION_ROUND: '修复轮',
  REVIEWING: '审查',
  AWAITING_AUTO_FALLBACK: '等待降级确认',
};

// "省 ≈Z%" from the shared token-savings payload shape
// (high_input_tokens + estimated_tokens_avoided = the估算 baseline).
function collabSavedPercent(data) {
  const avoided = Number(data.estimated_tokens_avoided) || 0;
  const high = Number(data.high_input_tokens) || 0;
  const baseline = high + avoided;
  if (!(baseline > 0) || !(avoided > 0)) return '';
  return '省 ≈' + Math.round((avoided / baseline) * 100) + '%';
}

function collabMarkerText(type, data) {
  if (!data || typeof data !== 'object') return '';
  try {
    const head = '⬢ ';
    const phase = String(data.phase || '');
    const reason = data.reason ? truncate(String(data.reason).replace(/\s+/gu, ' '), 300) : '';

    if (type === 'COLLAB_TASK_STARTED') {
      if (String(data.version || '') === 'v2') {
        const stage = phase === 'EXPLORING' || !phase
          ? '探索阶段(本地, 只读)'
          : (COLLAB_PHASE_LABELS[phase] || truncate(phase, 40)) + '阶段';
        return head + 'DragonAI Collab v2 · 任务启动 · ' + stage;
      }
      return head + 'DragonAI Collab · 任务启动 (' + truncate(String(data.mode || 'auto-collab'), 40) + ')';
    }

    if (type === 'COLLAB_EXPLORATION') {
      const leg = Number.isFinite(Number(data.leg)) ? Number(data.leg) : 0;
      const legText = leg > 0 ? ' · leg ' + leg : '';
      const step = Number.isFinite(Number(data.step)) && Number(data.step) > 0
        ? ' · step ' + Number(data.step)
        : '';
      const tool = data.tool ? ' · ' + truncate(String(data.tool), 60) : '';
      const preview = data.preview
        ? ' · ' + truncate(String(data.preview).replace(/\s+/gu, ' '), 300)
        : '';
      if (phase === 'leg_started') {
        const task = data.task_preview
          ? ' · ' + truncate(String(data.task_preview).replace(/\s+/gu, ' '), 200)
          : '';
        return head + '探索' + legText + ' 开始' + (data.resumed ? ' (续)' : '') + task;
      }
      if (phase === 'tool_call') return head + '探索' + legText + step + tool + preview;
      if (phase === 'observation') {
        const merged = Number(data.merged_count) > 1 ? ' ×' + Number(data.merged_count) : '';
        return head + '探索' + legText + step + tool + ' ⇢ ' +
          (data.is_error ? '✗ ' : '') + 'observation' + merged + preview;
      }
      if (phase === 'steered') {
        const message = truncate(String(data.message_preview || '').replace(/\s+/gu, ' '), 200);
        return head + '探索转向' + (data.auto ? ' (auto)' : '') + ' · "' + message + '"';
      }
      if (phase === 'leg_done') {
        const failed = data.status && data.status !== 'finished';
        const steps = Number.isFinite(Number(data.steps)) ? ' · ' + Number(data.steps) + ' steps' : '';
        return head + '探索' + legText + (failed
          ? ' ✗ ' + truncate(String(data.status), 40)
          : ' 完成') + steps + preview;
      }
      if (phase === 'done') {
        if (String(data.status || '') === 'paused') {
          return head + '探索暂停' + (reason ? ' · ' + reason : '');
        }
        const parts = [head + '探索完成'];
        if (Number(data.legs)) parts.push(Number(data.legs) + ' legs');
        if (Number.isFinite(Number(data.facts))) parts.push(Number(data.facts) + ' facts');
        if (Number.isFinite(Number(data.files))) parts.push(Number(data.files) + ' files');
        if (Number(data.risks)) parts.push(Number(data.risks) + ' risks');
        if (Number.isFinite(Number(data.open_questions))) {
          parts.push(Number(data.open_questions) + ' questions');
        }
        return parts.join(' · ');
      }
      return phase ? head + '探索 · ' + truncate(phase, 40) : '';
    }

    if (type === 'COLLAB_REPORT') {
      if (phase === 'completed' || data.report_kind === 'task-completion') {
        const lines = [];
        if (typeof data.rendered === 'string' && data.rendered.trim()) {
          lines.push(data.rendered.trim());
          lines.push('');
        }
        const ref = data.report_ref
          ? ' · artifact ' + truncate(String(data.report_ref), 80)
          : '';
        lines.push(head + '任务报告已生成 · 无需审批' + ref);
        return lines.join('\n');
      }
      if (phase && phase !== 'ready') return '';
      const lines = [];
      if (typeof data.rendered === 'string' && data.rendered.trim()) {
        lines.push(data.rendered.trim());
        lines.push('');
      }
      const revision = Number.isFinite(Number(data.revision))
        ? ' (revision ' + Number(data.revision) + ')'
        : '';
      lines.push(
        head + '升级报告待审' + revision +
        ' · 回复 **yes** 采用 / **no: <意见>** 修改'
      );
      return lines.join('\n');
    }

    if (type === 'COLLAB_PLAN_REVIEW') {
      const steps = Number.isFinite(Number(data.steps)) && Number(data.steps) > 0
        ? ' · ' + Number(data.steps) + ' steps'
        : '';
      const round = Number.isFinite(Number(data.review_round)) && Number(data.review_round) > 0
        ? '（第 ' + Number(data.review_round) + '/2 轮）'
        : '';
      return head + '计划待审批' + steps +
        ' · 回复 **yes** 开始实施 / **no: <原因>** 重新规划' + round;
    }

    if (type === 'COLLAB_DISPATCH') {
      const route = String(data.route || '');
      const llm = data.subagent_llm ? ' (模型: ' + truncate(String(data.subagent_llm), 60) + ')' : '';
      const target = route === 'subagent'
        ? 'OpenHands subagent' + llm
        : route === 'proxied-toolcall'
          ? '高智能 lane (proxied toolcall)'
          : route === 'codex-native'
            ? 'Codex native (本地 worker)'
            : truncate(route || 'unknown route', 40);
      const stepNumber = Number(data.step_number);
      const total = Number(data.total_steps);
      const position = Number.isFinite(stepNumber) && stepNumber > 0
        ? ' · step ' + stepNumber + (Number.isFinite(total) && total > 0 ? '/' + total : '')
        : '';
      const why = data.reason
        ? '\nwhy: ' + truncate(String(data.reason).replace(/\s+/gu, ' '), 500)
        : '';
      return head + '派发' + position + ' → ' + target + why;
    }

    if (type === 'COLLAB_REVIEW_DECISION' || type === 'REVIEW_DECISION') {
      const action = truncate(String(data.action || 'UNKNOWN'), 40);
      const parts = [head + '审查 · ' + action];
      if (type === 'COLLAB_REVIEW_DECISION' && data.final) parts.push('最终审查');
      if (action === 'REVISE' && type === 'COLLAB_REVIEW_DECISION') {
        const applied = Number(data.corrections_applied) || 0;
        parts.push('修复轮（剩 ' + Math.max(0, 1 - applied) + ' 次）');
      }
      if (data.summary) {
        parts.push(truncate(String(data.summary).replace(/\s+/gu, ' '), 300));
      }
      return parts.join(' · ');
    }

    if (type === 'TOKEN_SAVINGS_ESTIMATED') {
      const high = Number(data.high_input_tokens) || 0;
      const avoided = Number(data.estimated_tokens_avoided) || 0;
      const saved = collabSavedPercent(data);
      return head + 'Token 节省(估算) · high 输入 ' + high +
        ' · 基线 ' + (high + avoided) + (saved ? ' · ' + saved : ' · 省 ≈0%');
    }

    if (type === 'COLLAB_STABLE_AUTO_OFFER') {
      return head + 'Copilot 不可用' + (reason ? ' (' + reason + ')' : '') +
        ' · 回复 **yes** 转稳定 /route auto / **no** 取消';
    }

    if (type === 'COLLAB_TASK_COMPLETED') {
      const parts = [head + '✔ 协作任务完成', '审查通过'];
      if (Number(data.high_calls) || Number(data.local_calls)) {
        parts.push('high ' + (Number(data.high_calls) || 0) +
          ' · local ' + (Number(data.local_calls) || 0));
      }
      const saved = collabSavedPercent(data);
      if (saved) parts.push(saved + ' (estimated)');
      return parts.join(' · ');
    }

    if (type === 'COLLAB_TASK_BLOCKED') {
      const review = data.review && typeof data.review === 'object' ? data.review : {};
      const why = reason ||
        (review.summary ? truncate(String(review.summary).replace(/\s+/gu, ' '), 300) : '') ||
        (phase ? 'phase ' + truncate(phase, 40) : '未知原因');
      return head + '✗ 协作任务受阻 · ' + why;
    }

    if (type === 'COLLAB_PHASE_STARTED') {
      if (!phase) return '';
      const label = COLLAB_PHASE_LABELS[phase];
      const lead = data.lead ? ' · lead=' + truncate(String(data.lead), 60) : '';
      return head + '阶段 · ' + (label ? label + ' (' + truncate(phase, 40) + ')' : truncate(phase, 60)) + lead;
    }

    if (type === 'WORK_ORDER_CREATED') {
      const checks = Array.isArray(data.completion_checks) ? data.completion_checks.length : 0;
      return head + '工单已创建 · ' +
        truncate(String(data.objective || 'work order').replace(/\s+/gu, ' '), 200) +
        (data.risk ? ' · risk=' + truncate(String(data.risk), 20) : '') +
        (checks ? ' · ' + checks + ' checks' : '');
    }

    if (type === 'ROUND_COMPLETED') {
      const parts = [head + '轮次完成'];
      if (phase) parts.push(truncate(phase, 40));
      if (Array.isArray(data.completed_steps) && data.completed_steps.length) {
        parts.push(data.completed_steps.length + ' steps');
      }
      if (Array.isArray(data.verified_facts) && data.verified_facts.length) {
        parts.push(data.verified_facts.length + ' facts');
      }
      if (Array.isArray(data.remaining_gaps) && data.remaining_gaps.length) {
        parts.push(data.remaining_gaps.length + ' gaps');
      }
      return parts.join(' · ');
    }

    if (type === 'LOCAL_ASSIST_REQUESTED') {
      return head + '本地佐证请求' + (phase ? ' · ' + truncate(phase, 40) : '') +
        (data.request ? ' · ' + truncate(String(data.request).replace(/\s+/gu, ' '), 300) : '');
    }

    if (type === 'COLLAB_FALLBACK_REQUESTED') {
      const preserved = Array.isArray(data.preserved_artifacts)
        ? data.preserved_artifacts.length
        : 0;
      return head + '协作降级 · ' + (reason || '原因未知') +
        (preserved ? ' · 保留 ' + preserved + ' 个 artifact' : '');
    }

    return '';
  } catch {
    return '';
  }
}

// One completed proxied-LLM call (PROXIED_LLM_USED): purpose, model and
// token spend plus the approval authority, e.g.
// "⇑ Proxied LLM · collab-review · copilot-claude5 · 3.2k tokens (approved: report-gate)".
function proxiedUsedText(data) {
  if (!data || typeof data !== 'object') return '';
  try {
    const parts = ['⇑ Proxied LLM'];
    if (data.purpose) parts.push(truncate(String(data.purpose), 60));
    const model = data.model || data.lane;
    if (model) parts.push(truncate(String(model), 80));
    const tokens = (Number(data.prompt_tokens) || 0) +
      (Number(data.completion_tokens) || 0);
    const tokenText = tokens >= 1000
      ? (Math.round(tokens / 100) / 10) + 'k tokens'
      : tokens + ' tokens';
    parts.push(tokenText);
    let text = parts.join(' · ');
    if (data.approved_by) {
      text += ' (approved: ' + truncate(String(data.approved_by), 40) + ')';
    }
    return text;
  } catch {
    return '';
  }
}

function consultMarkerText(data) {
  if (!data || typeof data !== 'object') return '';
  if (String(data.kind || '') !== 'solution') return '';
  const marker = String(data.ui_marker || 'escalated to Perplexity (web)');
  const reason = data.reason
    ? '\nwhy: ' + truncate(String(data.reason), 500)
    : '';
  const state = data.ok === false ? ' · failed' : ' · advisory ready';
  return '⇑ ' + marker + state + reason;
}

function contractErrorText(data) {
  if (!data || typeof data !== 'object') return '';
  try {
    const tool = data.tool || data.name || 'unknown_tool';
    const lines = ['✗ Codex tool contract · ' + truncate(tool, 200)];
    if (data.error_code) lines.push('code: ' + truncate(data.error_code, 120));
    if (data.codex_error) lines.push('Codex says: ' + truncate(data.codex_error, 700));
    if (Array.isArray(data.argument_names) && data.argument_names.length) {
      lines.push('args: ' + truncate(data.argument_names.join(', '), 400));
    }
    if (Array.isArray(data.repairs) && data.repairs.length) {
      lines.push('repairs: ' + truncate(data.repairs.join('; '), 600));
    }
    return lines.join('\n');
  } catch {
    return '';
  }
}

function toolResultMarkerText(data) {
  if (!data || typeof data !== 'object' || !data.tool) return '';
  if (data.status !== 'completed' && data.status !== 'failed') return '';
  const duration = Number.isFinite(Number(data.duration_ms)) ? ' (' + Number(data.duration_ms) + 'ms)' : '';
  const failed = data.status === 'failed' ? ' failed' : '';
  const source = String(data.tool).startsWith('mcp__') ? 'MCP' : 'Tool';
  return '◆ ' + source + ' result · ' + data.tool + failed + duration;
}

function toolDecisionMarkerText(data) {
  if (!data || typeof data !== 'object' || !data.tool) return '';
  const source = String(data.tool).startsWith('mcp__') ? 'MCP' : 'Tool';
  const owner = data.decision === 'execute_local'
    ? 'DragonAI read-only runtime'
    : data.decision === 'execute_subagent'
      ? 'OpenHands subagent'
      : 'Codex approval/sandbox';
  return '◇ ' + source + ' call · ' + data.tool + ' → ' + owner;
}

function evidenceMarkerText(data) {
  if (!data || typeof data !== 'object' || !data.artifact_ref) return '';
  const count = Number(data.evidence_count) || 0;
  const summary = data.summary ? truncate(String(data.summary).replace(/\s+/gu, ' '), 900) : '';
  const path = data.artifact_path ? ', ' + String(data.artifact_path) : '';
  return 'DragonAI verified evidence (' + count + ', ' + data.artifact_ref + path + ')' + (summary ? '\n' + summary : '');
}

// Translate the Brain's streamed turn events into a Codex Responses SSE
// lifecycle on `res`. `upstreamBody` is the Brain's SSE body (async iterable).
async function streamTurn(res, upstreamBody, body, translateOutputItem, log) {
  const responseId = genId('resp');
  const createdAt = now();
  const model = (body && body.model) || '';
  const seq = { index: 0, num: 0 };
  const output = [];
  const emittedCallIds = new Set();
  const msg = { id: genId('msg'), started: false, done: false, index: -1, text: '' };
  let terminal = false;

  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
  });
  // Silence-gated heartbeat towards Codex: whenever no SSE *event* has been
  // written for `heartbeatMs`, emit a no-op `response.dragonai.heartbeat`
  // event. Codex parses it and drops it in its harmless catch-all branch
  // (`_ => trace!("unhandled responses event")` in codex-rs
  // codex-api/src/sse/responses.rs), but arriving at all resets its
  // `stream_idle_timeout_ms` timer. Real events reset the silence clock, so a
  // steadily streaming turn never emits heartbeats.
  const heartbeatMs = sseHeartbeatMs();
  const hb = { lastEventAt: Date.now(), sent: 0, timer: null };

  function writeEvent(event, obj) {
    hb.lastEventAt = Date.now();
    markers.writeSseEvent(res, event, obj);
  }

  function writeItem(item) {
    hb.lastEventAt = Date.now();
    markers.emitOutputItem(res, item, seq);
  }

  function stopHeartbeat() {
    if (hb.timer) {
      clearInterval(hb.timer);
      hb.timer = null;
      if (hb.sent > 0) log('brain: sse heartbeats emitted this turn: ' + hb.sent);
    }
  }

  const inProgress = { id: responseId, object: 'response', created_at: createdAt, status: 'in_progress', model, output: [], output_text: '' };
  writeEvent('response.created', { type: 'response.created', sequence_number: seq.num++, response: inProgress });
  writeEvent('response.in_progress', { type: 'response.in_progress', sequence_number: seq.num++, response: inProgress });
  if (heartbeatMs > 0) {
    // Tick faster than the interval so the worst-case silent gap stays close
    // to `heartbeatMs` (never `2 * heartbeatMs`).
    hb.timer = setInterval(() => {
      if (terminal || res.writableEnded) return;
      if (Date.now() - hb.lastEventAt < heartbeatMs) return;
      try {
        writeEvent('response.dragonai.heartbeat', {
          type: 'response.dragonai.heartbeat',
          sequence_number: seq.num++,
        });
        hb.sent += 1;
        log('brain: sse heartbeat #' + hb.sent + ' (silence > ' + heartbeatMs + 'ms)');
      } catch {}
    }, Math.max(25, Math.min(heartbeatMs, 5000)));
    if (typeof hb.timer.unref === 'function') hb.timer.unref();
  }

  function startMessage() {
    if (msg.started) return;
    msg.started = true;
    msg.index = seq.index++;
    writeEvent('response.output_item.added', {
      type: 'response.output_item.added',
      output_index: msg.index,
      sequence_number: seq.num++,
      item: { id: msg.id, type: 'message', status: 'in_progress', role: 'assistant', content: [] },
    });
    writeEvent('response.content_part.added', {
      type: 'response.content_part.added',
      item_id: msg.id,
      output_index: msg.index,
      content_index: 0,
      sequence_number: seq.num++,
      part: { type: 'output_text', text: '', annotations: [] },
    });
  }

  function emitDelta(text) {
    if (!text) return;
    startMessage();
    msg.text += text;
    writeEvent('response.output_text.delta', {
      type: 'response.output_text.delta',
      item_id: msg.id,
      output_index: msg.index,
      content_index: 0,
      sequence_number: seq.num++,
      delta: text,
    });
  }

  function finishMessage() {
    if (!msg.started || msg.done) return;
    msg.done = true;
    const item = messageItem(msg.id, msg.text);
    writeEvent('response.output_text.done', {
      type: 'response.output_text.done', item_id: msg.id, output_index: msg.index, content_index: 0, sequence_number: seq.num++, text: msg.text,
    });
    writeEvent('response.content_part.done', {
      type: 'response.content_part.done', item_id: msg.id, output_index: msg.index, content_index: 0, sequence_number: seq.num++, part: item.content[0],
    });
    writeEvent('response.output_item.done', {
      type: 'response.output_item.done', output_index: msg.index, sequence_number: seq.num++, item,
    });
    output.push(item);
  }

  function emitToolRequest(data) {
    if (!data || typeof data !== 'object' || !data.name) return;
    const callId = data.call_id || data.id || '';
    if (callId && emittedCallIds.has(callId)) return;
    if (callId) emittedCallIds.add(callId);
    const item = applyTranslate(functionCallItem(data), translateOutputItem);
    writeItem(item);
    output.push(item);
  }

  // Best-effort UI chip for PLAN_UPDATE / TOOL_EXECUTION: a small completed
  // message item (added+done pair, same shape sendSseCompleted streams).
  function emitTextMarker(text) {
    if (!text) return;
    const item = messageItem(genId('msg'), text);
    writeItem(item);
    output.push(item);
  }

  function emitFailed(message) {
    terminal = true;
    stopHeartbeat();
    writeEvent('response.failed', {
      type: 'response.failed',
      sequence_number: seq.num++,
      response: { id: responseId, object: 'response', created_at: createdAt, status: 'failed', model, output: output.slice(), error: { message } },
    });
    res.end();
  }

  function handleResult(data) {
    if (data && data.status === 'failed') {
      finishMessage();
      emitFailed(data.error ? String(data.error) : 'dragonai brain turn failed');
      return;
    }
    // Non-streamed final text (no MODEL_DELTA frames arrived).
    if (!msg.started && data && typeof data.text === 'string' && data.text) emitDelta(data.text);
    finishMessage();
    const toolRequests = data && Array.isArray(data.tool_requests) ? data.tool_requests : [];
    for (const t of toolRequests) emitToolRequest(t);
    terminal = true;
    stopHeartbeat();
    writeEvent('response.completed', {
      type: 'response.completed',
      sequence_number: seq.num++,
      response: {
        id: responseId,
        object: 'response',
        created_at: createdAt,
        status: 'completed',
        model,
        output: output.slice(),
        output_text: msg.text,
        usage: mapUsage(data && data.usage),
      },
    });
    res.end();
  }

  function handleFrame(type, data) {
    if (type === 'MODEL_DELTA') {
      emitDelta(data && typeof data.text === 'string' ? data.text : '');
    } else if (type === 'TOOL_REQUEST') {
      emitToolRequest(data);
    } else if (type === 'PLAN_UPDATE') {
      try {
        const parts = [planMarkerText(data)];
        if (data && typeof data === 'object') {
          if (data.planner && typeof data.planner === 'object' &&
              Array.isArray(data.planner.attempts) &&
              data.planner.attempts.length) {
            parts.push(plannerLadderText(data.planner));
          }
          if (data.phase === 'rejected' && data.blocked_reason) {
            parts.push('✗ plan blocked: ' + truncate(String(data.blocked_reason), 500));
          }
          if (data.plan && typeof data.plan === 'object') {
            parts.push(planTableText(data.plan));
          }
        }
        emitTextMarker(parts.filter(Boolean).join('\n'));
      } catch {}
    } else if (type === 'PLAN_STEP_UPDATE') {
      try { emitTextMarker(stepUpdateText(data)); } catch {}
    } else if (type === 'MEMORY_RECALL') {
      try { emitTextMarker(memoryRecallText(data)); } catch {}
    } else if (type === 'TOOL_RESULT') {
      try { emitTextMarker(toolResultMarkerText(data)); } catch {}
    } else if (type === 'TOOL_DECISION') {
      try { emitTextMarker(toolDecisionMarkerText(data)); } catch {}
    } else if (type === 'TOOL_CONTRACT_ERROR') {
      try { emitTextMarker(contractErrorText(data)); } catch {}
    } else if (type === 'SUBAGENT_EXECUTION' || type === 'FALLBACK_EXECUTION') {
      // S-P2 event name (old FALLBACK_EXECUTION kept for one-release compat).
      try { emitTextMarker(subagentMarkerText(data)); } catch {}
    } else if (COLLAB_MARKER_EVENTS.has(type)) {
      // Collab v1 + v2 events (P7) -> the ⬢ collab marker; empty renders
      // are skipped and a bad payload never breaks the stream.
      try { emitTextMarker(collabMarkerText(type, data)); } catch {}
    } else if (type === 'PROXIED_LLM_USED') {
      // Proxied-LLM usage log: every remote call renders a visible chip.
      try { emitTextMarker(proxiedUsedText(data)); } catch {}
    } else if (type === 'CONSULT') {
      // Whole-solution Perplexity escalation remains advisory; the next
      // local/Codex round still owns every tool call.
      try { emitTextMarker(consultMarkerText(data)); } catch {}
    } else if (type === 'EVIDENCE_SUMMARY') {
      try { emitTextMarker(evidenceMarkerText(data)); } catch {}
    } else if (type === 'MODEL_RESULT') {
      handleResult(data);
    }
    // BRAIN_REASONING_REQUEST and any unknown event types are
    // observability-only: tolerated and ignored, never breaking the stream.
  }

  try {
    let buffer = '';
    const decoder = new StringDecoder('utf8');
    for await (const chunk of upstreamBody) {
      buffer += decoder.write(Buffer.from(chunk));
      const blocks = buffer.split(/\r?\n\r?\n/u);
      buffer = blocks.pop() || '';
      for (const block of blocks) {
        const frame = parseSseBlock(block);
        if (!frame.data && /^\s*:/mu.test(block)) {
          if (!terminal && !res.writableEnded) res.write(': dragonai-heartbeat\n\n');
          continue;
        }
        if (!frame.data || frame.data === '[DONE]') continue;
        let data;
        try { data = JSON.parse(frame.data); } catch { continue; }
        const type = frame.event || (data && (data.event || data.type)) || '';
        handleFrame(type, data);
        if (terminal) return;
      }
    }
    buffer += decoder.end();
  } catch (e) {
    stopHeartbeat();
    log('brain: stream error: ' + e.message);
    if (!terminal) emitFailed('dragonai brain stream error: ' + e.message);
    return;
  }
  if (!terminal) {
    stopHeartbeat();
    log('brain: stream ended without MODEL_RESULT');
    emitFailed('dragonai brain stream ended without MODEL_RESULT');
  }
}

// Non-streaming turn: single JSON MODEL_RESULT envelope -> Responses JSON.
function respondJsonResult(res, envelope, body, translateOutputItem) {
  const payload = envelope && envelope.event === 'MODEL_RESULT' && envelope.payload && typeof envelope.payload === 'object'
    ? envelope.payload
    : null;
  if (!payload) {
    sendError(res, 'dragonai brain returned an unexpected envelope');
    return;
  }
  if (payload.status === 'failed') {
    sendError(res, payload.error ? String(payload.error) : 'dragonai brain turn failed');
    return;
  }
  const output = [];
  const toolRequests = Array.isArray(payload.tool_requests) ? payload.tool_requests : [];
  for (const t of toolRequests) {
    if (t && typeof t === 'object' && t.name) output.push(applyTranslate(functionCallItem(t), translateOutputItem));
  }
  const text = typeof payload.text === 'string' ? payload.text : '';
  if (text || output.length === 0) output.push(messageItem(genId('msg'), text));
  const response = {
    id: genId('resp'),
    object: 'response',
    created_at: now(),
    status: 'completed',
    model: (body && body.model) || '',
    output,
    output_text: text,
    usage: mapUsage(payload.usage),
  };
  const raw = JSON.stringify(response);
  res.writeHead(200, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(raw),
  });
  res.end(raw);
}

// Handle one POST /responses turn through the Brain. Never throws; on any
// failure it answers 502 JSON (before the SSE starts) or response.failed
// (after), so the client never hangs.
async function runBrainTurn({ req, res, body, isStream, toolRegistry, translateOutputItem, log } = {}) {
  const logFn = typeof log === 'function' ? log : () => {};
  try {
    const envelope = buildModelRequest(body || {}, {
      stream: Boolean(isStream),
      headers: req && req.headers ? req.headers : {},
      toolRegistry,
    });
    const headers = { 'content-type': 'application/json' };
    if (brainApiKey()) headers.authorization = 'Bearer ' + brainApiKey();
    if (isStream) headers.accept = 'text/event-stream';

    let upstream;
    try {
      upstream = await fetch(brainUrl() + '/agent/v1/turn', {
        method: 'POST',
        headers,
        body: JSON.stringify(envelope),
      });
    } catch (e) {
      logFn('brain: turn request failed: ' + e.message);
      sendError(res, 'dragonai brain unreachable: ' + e.message);
      return;
    }
    if (!upstream.ok) {
      const detail = await upstream.text().catch(() => '');
      logFn('brain: turn failed with status ' + upstream.status);
      sendError(res, 'dragonai brain error ' + upstream.status + (detail ? ': ' + truncate(detail) : ''));
      return;
    }

    if (!isStream) {
      let json;
      try {
        json = await upstream.json();
      } catch (e) {
        sendError(res, 'dragonai brain returned invalid JSON: ' + e.message);
        return;
      }
      respondJsonResult(res, json, body, translateOutputItem);
      return;
    }

    await streamTurn(res, upstream.body, body, translateOutputItem, logFn);
  } catch (e) {
    logFn('brain: turn failed: ' + e.message);
    if (!res.headersSent) {
      sendError(res, 'dragonai brain turn failed: ' + e.message);
    } else {
      try {
        markers.writeSseEvent(res, 'response.failed', {
          type: 'response.failed',
          response: { object: 'response', status: 'failed', error: { message: 'dragonai brain turn failed: ' + e.message } },
        });
      } catch {}
      res.end();
    }
  }
}

// Forward GET /v1/models to the Brain (health checks, Codex model probes).
async function modelsProxy(res) {
  try {
    const headers = {};
    if (brainApiKey()) headers.authorization = 'Bearer ' + brainApiKey();
    const upstream = await fetch(brainUrl() + '/v1/models', { headers });
    const text = await upstream.text();
    res.writeHead(upstream.status, { 'content-type': upstream.headers.get('content-type') || 'application/json' });
    res.end(text);
  } catch (e) {
    sendError(res, 'dragonai brain unreachable: ' + e.message);
  }
}

module.exports = {
  brainUrl,
  enabled,
  conversationId,
  transcriptHash,
  codexTurnMetadata,
  buildModelRequest,
  runBrainTurn,
  modelsProxy,
  // Pure marker renderers (unit-tested in test/dragonai-brain-markers.test.js).
  planTableText,
  plannerLadderText,
  memoryRecallText,
  stepUpdateText,
  contractErrorText,
  subagentMarkerText,
  collabMarkerText,
  toolDecisionMarkerText,
  proxiedUsedText,
};
