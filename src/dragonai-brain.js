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
      tool_choice: src.tool_choice || 'auto',
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

function planMarkerText(data) {
  if (!data || typeof data !== 'object') return '';
  const lane = data.lane || '';
  const model = data.model || '';
  if (!lane && !model) return '';
  const lines = [];
  const details = data.neural_route && typeof data.neural_route === 'object'
    ? data.neural_route : {};
  const plan = data.turn_plan && typeof data.turn_plan === 'object'
    ? data.turn_plan : {};
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
    if (
      !data.is_continuation &&
      Array.isArray(toolProfile.selected) &&
      toolProfile.selected.length
    ) {
      lines.push(
        'selected: ' +
        truncate(
          toolProfile.selected.map(friendlyTool).filter(Boolean).join(' · '),
          520,
        )
      );
    }
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
  const inProgress = { id: responseId, object: 'response', created_at: createdAt, status: 'in_progress', model, output: [], output_text: '' };
  markers.writeSseEvent(res, 'response.created', { type: 'response.created', sequence_number: seq.num++, response: inProgress });
  markers.writeSseEvent(res, 'response.in_progress', { type: 'response.in_progress', sequence_number: seq.num++, response: inProgress });
  const heartbeatMs = Math.max(1000, Number(process.env.DRAGONAI_SSE_HEARTBEAT_MS) || 15000);
  const heartbeat = setInterval(() => {
    if (!terminal && !res.writableEnded) {
      try {
        // A real Responses event also resets clients whose idle detector only
        // counts parsed SSE events (rather than raw comment bytes).
        markers.writeSseEvent(res, 'response.in_progress', {
          type: 'response.in_progress',
          sequence_number: seq.num++,
          response: { ...inProgress, status: 'in_progress' },
        });
      } catch {}
    }
  }, heartbeatMs);
  if (typeof heartbeat.unref === 'function') heartbeat.unref();

  function startMessage() {
    if (msg.started) return;
    msg.started = true;
    msg.index = seq.index++;
    markers.writeSseEvent(res, 'response.output_item.added', {
      type: 'response.output_item.added',
      output_index: msg.index,
      sequence_number: seq.num++,
      item: { id: msg.id, type: 'message', status: 'in_progress', role: 'assistant', content: [] },
    });
    markers.writeSseEvent(res, 'response.content_part.added', {
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
    markers.writeSseEvent(res, 'response.output_text.delta', {
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
    markers.writeSseEvent(res, 'response.output_text.done', {
      type: 'response.output_text.done', item_id: msg.id, output_index: msg.index, content_index: 0, sequence_number: seq.num++, text: msg.text,
    });
    markers.writeSseEvent(res, 'response.content_part.done', {
      type: 'response.content_part.done', item_id: msg.id, output_index: msg.index, content_index: 0, sequence_number: seq.num++, part: item.content[0],
    });
    markers.writeSseEvent(res, 'response.output_item.done', {
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
    markers.emitOutputItem(res, item, seq);
    output.push(item);
  }

  // Best-effort UI chip for PLAN_UPDATE / TOOL_EXECUTION: a small completed
  // message item (added+done pair, same shape sendSseCompleted streams).
  function emitTextMarker(text) {
    if (!text) return;
    const item = messageItem(genId('msg'), text);
    markers.emitOutputItem(res, item, seq);
    output.push(item);
  }

  function emitFailed(message) {
    terminal = true;
    clearInterval(heartbeat);
    markers.writeSseEvent(res, 'response.failed', {
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
    clearInterval(heartbeat);
    markers.writeSseEvent(res, 'response.completed', {
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
      try { emitTextMarker(planMarkerText(data)); } catch {}
    } else if (type === 'TOOL_RESULT') {
      try { emitTextMarker(toolResultMarkerText(data)); } catch {}
    } else if (type === 'TOOL_DECISION') {
      try { emitTextMarker(toolDecisionMarkerText(data)); } catch {}
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
    clearInterval(heartbeat);
    log('brain: stream error: ' + e.message);
    if (!terminal) emitFailed('dragonai brain stream error: ' + e.message);
    return;
  }
  if (!terminal) {
    clearInterval(heartbeat);
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
async function runBrainTurn({ req, res, body, isStream, translateOutputItem, log } = {}) {
  const logFn = typeof log === 'function' ? log : () => {};
  try {
    const envelope = buildModelRequest(body || {}, {
      stream: Boolean(isStream),
      headers: req && req.headers ? req.headers : {},
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
};
