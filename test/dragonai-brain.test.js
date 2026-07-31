'use strict';

const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const brain = require('../src/dragonai-brain');

function listen(server) {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve(server.address().port));
  });
}

function close(server) {
  return new Promise((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
}

function writeSse(res, event, data) {
  if (event) res.write('event: ' + event + '\n');
  res.write('data: ' + JSON.stringify(data) + '\n\n');
}

function parseSse(body) {
  return body.split(/\r?\n\r?\n/).filter(Boolean).map((block) => {
    const lines = block.split(/\r?\n/);
    const event = lines.find((line) => line.startsWith('event:'));
    const data = lines.filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart()).join('\n');
    return {
      event: event ? event.slice(6).trim() : null,
      data: data === '[DONE]' ? data : JSON.parse(data),
    };
  });
}

function postStream(port, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1',
      port,
      method: 'POST',
      path: '/v1/responses',
      headers: {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(payload),
        accept: 'text/event-stream',
      },
    }, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => resolve({
        statusCode: res.statusCode,
        body: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    req.end(payload);
  });
}

test('brain envelope preserves stable conversation, task, and fork lineage headers', () => {
  const body = {
    instructions: 'You are Codex.',
    input: [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Continue the fix' }] }],
  };
  const envelope = brain.buildModelRequest(body, {
    headers: {
      'x-dragonai-conversation-id': 'codex-thread-abc123',
      'x-dragonai-backend-session-id': 'backend-7',
      'x-dragonai-task-id': 'task-9',
      'x-dragonai-parent-conversation-id': 'codex-thread-parent',
      'x-dragonai-conversation-operation': 'fork',
    },
  });
  assert.equal(envelope.conversation_id, 'codex-thread-abc123');
  assert.equal(envelope.payload.metadata.backend_session_id, 'backend-7');
  assert.equal(envelope.payload.metadata.task_id, 'task-9');
  assert.equal(envelope.payload.metadata.parent_conversation_id, 'codex-thread-parent');
  assert.equal(envelope.payload.metadata.operation, 'fork');
  assert.match(envelope.payload.metadata.transcript_hash, /^[0-9a-f]{64}$/);
});

test('brain envelope falls back to Codex environment cwd metadata', () => {
  const envelope = brain.buildModelRequest({
    instructions: '<environment_context><cwd>/tmp/codex-instruction-worktree</cwd></environment_context>',
    input: [{ type: 'message', role: 'user', content: 'Stage this only' }],
  });
  assert.equal(
    envelope.payload.metadata.workspace,
    '/tmp/codex-instruction-worktree',
  );
});

test('Codex native thread and turn metadata are the primary stable identity', () => {
  const native = {
    session_id: 'native-session',
    thread_id: 'native-thread',
    turn_id: 'native-turn',
    forked_from_thread_id: 'parent-thread',
    workspaces: {
      '/tmp/codex-clean-worktree': { has_changes: false },
      '/tmp/codex-active-worktree': { has_changes: true },
    },
  };
  const envelope = brain.buildModelRequest(
    {
      instructions: 'Compacted instructions may change.',
      input: [{ type: 'message', role: 'user', content: 'Resume after compact' }],
    },
    {
      headers: {
        'thread-id': 'native-thread',
        'session-id': 'native-session',
        'x-codex-turn-metadata': JSON.stringify(native),
        'x-dragonai-conversation-id': 'legacy-override',
      },
    },
  );
  assert.equal(envelope.conversation_id, 'native-thread');
  assert.equal(envelope.payload.metadata.backend_session_id, 'native-session');
  assert.equal(envelope.payload.metadata.task_id, 'native-turn');
  assert.equal(envelope.payload.metadata.parent_conversation_id, 'parent-thread');
  assert.equal(envelope.payload.metadata.operation, 'fork');
  assert.equal(envelope.payload.metadata.workspace, '/tmp/codex-active-worktree');
});

// One compact end-to-end test: a fake DragonAI Brain streams a full
// dragonai-agent/v1 turn (including observability frames and one unknown
// event type) and the real proxy request path must translate it into a valid
// Codex Responses SSE lifecycle. Passthrough behavior with
// DRAGONAI_BRAIN_URL unset is covered by test/proxy.test.js.
test('brain mode translates a streamed dragonai-agent/v1 turn into a Codex Responses SSE lifecycle', async () => {
  let brainRequest = null;
  const brainServer = http.createServer((req, res) => {
    assert.equal(req.method, 'POST');
    assert.equal(req.url, '/agent/v1/turn');
    assert.equal(req.headers.authorization, 'Bearer test-brain-key');
    assert.equal(req.headers.accept, 'text/event-stream');
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      brainRequest = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      writeSse(res, 'PLAN_UPDATE', {
        turn_id: brainRequest.turn_id,
        is_continuation: true,
        lane: 'local',
        model: 'qwen2.5-coder-14b',
        reason: 'read-heavy debugging turn',
        planner: {
          source: '14b-router',
          actual_model: 'dragonai-expert-planner-14b-v1-mlx',
          recommendation: 'GATHER_EVIDENCE',
          task_family: 'debug',
          risk: 'low',
          confidence: 0.91,
          can_choose_provider: false,
        },
        neural_route: {
          configured_model: 'qwen3-coder-30b-worker',
          actual_model: 'qwen3-coder-30b-a3b-instruct-mlx@4bit',
          model_resolution: 'lmstudio-loaded-alias',
          action: 'LOCAL_PROBE',
          requested_action: 'GATHER_EVIDENCE',
          final_action: 'GATHER_EVIDENCE',
          policy: 'adaptive',
          mode: 'active',
          bundle_version: 'dragonai-brain-v1',
          probabilities: {
            local_direct: 0.1,
            local_probe: 0.3,
            high_direct: 0.5,
            gather_evidence: 0.1,
          },
        },
        tool_profile: {
          profile: 'repository-edit',
          available_count: 96,
          available_tokens: 44290,
          selected_count: 2,
          selected_tokens: 1560,
          dropped_count: 94,
          dropped_tokens: 42730,
          selected: [
            { name: 'shell', source: 'codex/builtin-or-configured', tokens: 920 },
            { name: 'apply_patch', source: 'codex/builtin-or-configured', tokens: 640 },
          ],
          largest_dropped: [
            { name: 'mcp__browser__snapshot', source: 'mcp:browser', tokens: 4100 },
          ],
        },
      });
      writeSse(res, 'PLAN_UPDATE', {
        turn_id: brainRequest.turn_id,
        lane: 'local',
        model: 'qwen2.5-coder-14b',
        is_continuation: true,
        stage_intent: {
          status: 'delegating',
          source: 'formal-tool-call',
          selected_paths: ['src/PaymentService.java'],
          mixed_paths: [],
          command: 'git add -- src/PaymentService.java',
          reason: 'Preserved and validated the LLM request',
          workspace: '/tmp/codex-active-worktree',
        },
      });
      writeSse(res, 'PLAN_UPDATE', {
        turn_id: brainRequest.turn_id,
        lane: 'local',
        model: 'qwen2.5-coder-14b',
        is_continuation: true,
        stage_failure: {
          status: 'not_executed',
          reason: 'Codex returned no verifiable git-add result',
          manual_command: 'git add -- src/PaymentService.java',
          fallback_instruction: 'DragonAI fallback: execute',
        },
      });
      writeSse(res, 'TOOL_DECISION', { turn_id: brainRequest.turn_id, call_id: 'call_2', tool: 'shell', decision: 'execute_local', reason: 'read-only grep' });
      writeSse(res, 'MODEL_DELTA', { turn_id: brainRequest.turn_id, text: 'Payment retry ' });
      writeSse(res, 'MODEL_DELTA', { turn_id: brainRequest.turn_id, text: 'bug found.' });
      writeSse(res, 'TOOL_RESULT', { turn_id: brainRequest.turn_id, call_id: 'call_2', tool: 'shell', status: 'completed', duration_ms: 41 });
      writeSse(res, 'EVIDENCE_SUMMARY', {
        turn_id: brainRequest.turn_id,
        artifact_ref: 'sha256:abc123',
        evidence_count: 1,
        summary: 'src/PaymentService.java verified at revision deadbeef',
      });
      writeSse(res, 'TOOL_REQUEST', { turn_id: brainRequest.turn_id, id: 'fc_7', call_id: 'call_7', name: 'apply_patch', arguments: '{"patch":"*** Begin Patch"}' });
      writeSse(res, 'TOOL_REQUEST', { turn_id: brainRequest.turn_id, id: 'fc_8', call_id: 'call_8', name: 'shell', arguments: '{"command":["pytest","-q"]}' });
      writeSse(res, 'FUTURE_UNKNOWN_EVENT', { turn_id: brainRequest.turn_id, anything: true });
      writeSse(res, 'MODEL_RESULT', {
        turn_id: brainRequest.turn_id,
        status: 'completed',
        text: 'Payment retry bug found.',
        turn_terminal: false,
        tool_transaction: {
          state: 'awaiting_outputs',
          call_ids: ['call_7', 'call_8'],
          parallel: true,
        },
        tool_requests: [
          { id: 'fc_7', call_id: 'call_7', name: 'apply_patch', arguments: '{"patch":"*** Begin Patch"}' },
          { id: 'fc_8', call_id: 'call_8', name: 'shell', arguments: '{"command":["pytest","-q"]}' },
        ],
        usage: { input_tokens: 5210, output_tokens: 240 },
      });
      res.end();
    });
  });
  const brainPort = await listen(brainServer);

  // Real proxy request path, pointed at an upstream that must never be hit.
  const upstream = http.createServer((req, res) => {
    res.writeHead(500);
    res.end('upstream must not be called in brain mode');
  });
  const upstreamPort = await listen(upstream);
  const codexHome = fs.mkdtempSync(path.join(os.tmpdir(), 'codex-proxy-brain-test-'));
  fs.mkdirSync(path.join(codexHome, 'ollama-shape-proxy'), { recursive: true });
  fs.writeFileSync(path.join(codexHome, 'ollama-shape-proxy', 'proxy-models.toml'), [
    'text_model = "test-model"',
    `upstream_url = "http://127.0.0.1:${upstreamPort}/custom"`,
    '',
  ].join('\n'));

  const previousEnv = {
    CODEX_HOME: process.env.CODEX_HOME,
    PROXY_PORT: process.env.PROXY_PORT,
    DRAGONAI_BRAIN_URL: process.env.DRAGONAI_BRAIN_URL,
    DRAGONAI_BRAIN_API_KEY: process.env.DRAGONAI_BRAIN_API_KEY,
  };
  process.env.CODEX_HOME = codexHome;
  process.env.PROXY_PORT = '0';
  process.env.DRAGONAI_BRAIN_URL = 'http://127.0.0.1:' + brainPort;
  process.env.DRAGONAI_BRAIN_API_KEY = 'test-brain-key';
  delete require.cache[require.resolve('../src/proxy')];
  const proxy = require('../src/proxy');
  const server = proxy.startServer(0);
  await new Promise((resolve) => server.once('listening', resolve));

  try {
    assert.equal(brain.enabled(), true);
    const result = await postStream(server.address().port, {
      model: 'gpt-5.3-codex',
      instructions: 'You are Codex.',
      stream: true,
      parallel_tool_calls: true,
      tool_choice: 'auto',
      tools: [
        { type: 'function', name: 'shell', description: 'run a command', parameters: { type: 'object', properties: {} } },
        { type: 'function', name: 'apply_patch', description: 'apply a patch', parameters: { type: 'object', properties: {} } },
      ],
      input: [
        { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Fix payment bug' }] },
        { type: 'function_call', call_id: 'call_1', name: 'shell', arguments: '{"command":["grep","-r","PaymentService","."]}' },
        { type: 'function_call_output', call_id: 'call_1', output: 'src/PaymentService.java' },
      ],
    });
    assert.equal(result.statusCode, 200);
    const events = parseSse(result.body);
    const names = events.map((e) => e.event);

    // Valid Responses lifecycle: created/in_progress first, completed last, no failure.
    assert.equal(names[0], 'response.created');
    assert.equal(names[1], 'response.in_progress');
    assert.equal(names.at(-1), 'response.completed');
    assert.equal(names.filter((n) => n === 'response.completed').length, 1);
    assert.equal(names.includes('response.failed'), false);

    // MODEL_DELTA -> output_text.delta inside one message item lifecycle.
    const deltas = events.filter((e) => e.event === 'response.output_text.delta').map((e) => e.data.delta);
    assert.deepEqual(deltas, ['Payment retry ', 'bug found.']);
    assert.equal(events.filter((e) => e.event === 'response.content_part.added').length, 1);
    assert.equal(events.find((e) => e.event === 'response.output_text.done').data.text, 'Payment retry bug found.');
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('sha256:abc123')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('tools: 2/96 selected')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('| # | Runtime | Selected tool |')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('`apply_patch`')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('model resolved: qwen3-coder-30b-worker')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('planner=dragonai-expert-planner-14b-v1-mlx')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('probabilities local=0.10')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('policy=adaptive')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('◇ LLM staging intent · delegating')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('Codex transaction: git add -- src/PaymentService.java')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('workspace: /tmp/codex-active-worktree')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('failed/manual command: git add -- src/PaymentService.java')));
    assert.ok(events.some((e) => JSON.stringify(e.data).includes('DragonAI fallback: reply')));
    assert.equal(events.some((e) => JSON.stringify(e.data).includes('largest dropped tools')), false);

    // TOOL_REQUEST -> function_call item with preserved call_id, exactly once
    // even though MODEL_RESULT repeats it.
    const fnDone = events.filter((e) => e.event === 'response.output_item.done' && e.data.item.type === 'function_call');
    assert.equal(fnDone.length, 2);
    assert.equal(fnDone[0].data.item.call_id, 'call_7');
    assert.equal(fnDone[0].data.item.name, 'apply_patch');
    assert.equal(fnDone[0].data.item.arguments, '{"patch":"*** Begin Patch"}');
    assert.equal(fnDone[1].data.item.call_id, 'call_8');
    assert.equal(fnDone[1].data.item.name, 'shell');
    assert.equal(fnDone[1].data.item.arguments, '{"command":["pytest","-q"]}');
    assert.notEqual(fnDone[0].data.output_index, fnDone[1].data.output_index);

    // Terminal response.completed: mapped usage plus message + function_call output.
    const completed = events.find((e) => e.event === 'response.completed').data.response;
    assert.equal(completed.status, 'completed');
    assert.deepEqual(completed.usage, { input_tokens: 5210, output_tokens: 240, total_tokens: 5450 });
    assert.equal(completed.output.filter((item) => item.type === 'function_call').length, 2);
    assert.ok(completed.output.some((item) => item.type === 'message' && item.content[0].text === 'Payment retry bug found.'));
    assert.equal(completed.output_text, 'Payment retry bug found.');

    // The brain received a CODEX_MODEL_REQUEST envelope with converted messages/tools.
    assert.equal(brainRequest.protocol, 'dragonai-agent/v1');
    assert.equal(brainRequest.event, 'CODEX_MODEL_REQUEST');
    assert.match(brainRequest.turn_id, /^t-[0-9a-f]+$/);
    assert.match(brainRequest.conversation_id, /^codex-[0-9a-f]{24}$/);
    assert.equal(brainRequest.payload.model_hint, 'gpt-5.3-codex');
    assert.equal(brainRequest.payload.instructions, 'You are Codex.');
    assert.equal(brainRequest.payload.stream, true);
    assert.equal(brainRequest.payload.parallel_tool_calls, true);
    assert.deepEqual(brainRequest.payload.messages.map((m) => m.role), ['user', 'assistant', 'tool']);
    assert.equal(brainRequest.payload.messages[1].tool_calls[0].id, 'call_1');
    // The proxy may inject its own function tools (tool_search, web_search, …)
    // during translation; the request tools must still be there, nested.
    const toolNames = brainRequest.payload.tools.map((t) => t.function.name);
    assert.ok(toolNames.includes('shell'));
    assert.ok(toolNames.includes('apply_patch'));
    assert.ok(brainRequest.payload.tools.every((t) => t.type === 'function' && t.function.parameters));
  } finally {
    await close(server);
    await close(upstream);
    await close(brainServer);
    delete require.cache[require.resolve('../src/proxy')];
    for (const [key, value] of Object.entries(previousEnv)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(codexHome, { recursive: true, force: true });
  }
});
