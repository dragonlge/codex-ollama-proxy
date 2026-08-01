'use strict';

// Pure-function snapshot assertions for the DragonAI Brain UI marker
// renderers. These functions must never throw and must render stable
// Markdown/text chips from structured event payloads.

const test = require('node:test');
const assert = require('node:assert/strict');

const brain = require('../src/dragonai-brain');

test('planTableText renders the plan headline and Markdown step table', () => {
  const text = brain.planTableText({
    codex_plan: 'synced',
    planner_model: 'qwen-14b',
    table_rows: [
      { n: 1, glyph: '✔', status: 'completed', step: 'Patch the retry module', tool: 'apply_patch', owner: 'codex', attempts: 1, note: '' },
      { n: 2, glyph: '▶', status: 'in_progress', step: 'Run the tests', tool: 'exec_command', owner: 'codex', attempts: 2, note: 'flaky | run' },
    ],
  });
  assert.equal(text, [
    '◈ DragonAI Plan · 2 steps (1 done) · planner=qwen-14b · codex update_plan=synced',
    '',
    '| # | St | Step | Tool | Owner | Try | Note |',
    '|---:|:--:|---|---|---|---:|---|',
    '| 1 | ✔ | Patch the retry module | `apply_patch` | codex | 1 |  |',
    '| 2 | ▶ | Run the tests | `exec_command` | codex | 2 | flaky \\| run |',
  ].join('\n'));
  assert.equal(brain.planTableText({ table_rows: [] }), '');
  assert.equal(brain.planTableText(null), '');
});

test('plannerLadderText renders the escalation chain with complexity', () => {
  const text = brain.plannerLadderText({
    attempts: [
      { tier: 'router-14b', ok: false, reason: 'parse_failed' },
      { tier: 'worker-30b', ok: true },
    ],
    complexity: { score: 0.72, tier_floor: 'worker-30b' },
    confidence: 0.61,
    confidence_source: 'planner',
  });
  assert.equal(
    text,
    'planner ladder: 14b(parse_failed) → 30b(ok) · complexity=0.72 · ' +
    'floor=worker-30b · confidence=0.61 (planner)'
  );
  assert.equal(brain.plannerLadderText({ attempts: [] }), '');
  assert.equal(brain.plannerLadderText(undefined), '');
});

test('contractErrorText renders the Codex-verbatim rejection without values', () => {
  const text = brain.contractErrorText({
    tool: 'exec_command',
    error_code: 'missing_capability',
    codex_error: 'unsupported call: exec_command',
    argument_names: ['cmd', 'workdir'],
    repairs: ['joined argv list field cmd'],
  });
  assert.equal(text, [
    '✗ Codex tool contract · exec_command',
    'code: missing_capability',
    'Codex says: unsupported call: exec_command',
    'args: cmd, workdir',
    'repairs: joined argv list field cmd',
  ].join('\n'));
  assert.equal(brain.contractErrorText(null), '');
});

test('subagentMarkerText renders the local-tier phases with the legacy fallback headline', () => {
  const base = {
    codex_tools_used: false,
    capability: 'git_stage',
    mutating: true,
    approval: "requires the user approval phrase 'DragonAI fallback: execute'",
    owner: 'DragonAI local runtime',
    reason: "this turn's Codex tool registry exposes no exec capability",
    runtime: 'local',
  };
  const offer = brain.subagentMarkerText({
    ...base,
    phase: 'offer',
    approval_phrase: 'DragonAI fallback: execute',
  });
  assert.equal(offer, [
    '⚠ 未使用 Codex 提供的工具 — DragonAI 本地 fallback',
    "capability: git_stage (mutating, requires the user approval phrase 'DragonAI fallback: execute')",
    "why: this turn's Codex tool registry exposes no exec capability",
    'owner: DragonAI local runtime',
    '◇ offered · will NOT run until you reply exactly: `DragonAI fallback: execute`',
  ].join('\n'));

  const readonlyOffer = brain.subagentMarkerText({
    codex_tools_used: false,
    capability: 'git_status',
    mutating: false,
    approval: 'not required (read-only)',
    owner: 'DragonAI local runtime',
    reason: 'Codex rejected this step 2 times',
    runtime: 'local',
    phase: 'offer',
  });
  assert.match(readonlyOffer, /capability: git_status \(read-only, not required \(read-only\)\)/u);
  assert.match(readonlyOffer, /◇ offered · read-only, executing now/u);

  // Legacy FALLBACK_EXECUTION payloads (no runtime field) render as local.
  assert.match(
    brain.subagentMarkerText({ capability: 'git_status', phase: 'executing' }),
    /^⚠ 未使用 Codex 提供的工具 — DragonAI 本地 fallback · git_status → executing$/u
  );
  assert.equal(
    brain.subagentMarkerText({ ...base, phase: 'done', duration_ms: 12, output_preview: 'M related.py' }),
    '⚠ 未使用 Codex 提供的工具 — DragonAI 本地 fallback · git_stage → done (12ms) · M related.py'
  );
  assert.match(
    brain.subagentMarkerText({ ...base, phase: 'failed', output_preview: 'not a git repository' }),
    /git_stage → failed · not a git repository/u
  );
  assert.equal(brain.subagentMarkerText({}), '');
  assert.equal(brain.subagentMarkerText(null), '');
});

test('subagentMarkerText renders the openhands direct-call phases', () => {
  const base = {
    codex_tools_used: false,
    capability: 'exec_readonly',
    mutating: false,
    approval: 'not required (read-only)',
    owner: 'OpenHands subagent',
    reason: "this turn's Codex tool registry exposes no exec capability",
    runtime: 'openhands',
  };
  const offer = brain.subagentMarkerText({ ...base, phase: 'offer' });
  assert.equal(offer, [
    '⬡ OpenHands subagent',
    '⚠ 未使用 Codex 提供的工具',
    'capability: exec_readonly (read-only, not required (read-only))',
    "why: this turn's Codex tool registry exposes no exec capability",
    'owner: OpenHands subagent',
    '◇ offered · read-only, executing now',
  ].join('\n'));

  const mutatingOffer = brain.subagentMarkerText({
    ...base,
    capability: 'file_edit',
    mutating: true,
    approval: "requires the user approval phrase 'DragonAI subagent: execute'",
    phase: 'offer',
    approval_phrase: 'DragonAI subagent: execute',
  });
  assert.match(mutatingOffer, /⚠ 未使用 Codex 提供的工具/u);
  assert.match(
    mutatingOffer,
    /◇ offered · will NOT run until you reply exactly: `DragonAI subagent: execute`/u
  );

  assert.equal(
    brain.subagentMarkerText({ ...base, phase: 'executing' }),
    '⬡ OpenHands subagent · exec_readonly → executing'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'done', duration_ms: 812, output_preview: '3 files changed',
    }),
    '⬡ OpenHands subagent · exec_readonly → done (812ms) · 3 files changed'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'failed', duration_ms: 40, output_preview: 'denied',
    }),
    '⬡ OpenHands subagent · exec_readonly → failed (40ms) · denied'
  );
});

test('subagentMarkerText renders the delegated-task phases (S-P3 frames)', () => {
  const base = { runtime: 'openhands', owner: 'OpenHands subagent' };
  assert.equal(
    brain.subagentMarkerText({ ...base, phase: 'started', preview: 'fix the retry tests' }),
    '⬡ OpenHands subagent · 任务开始 · fix the retry tests\n⚠ 未使用 Codex 提供的工具'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'tool_call', step: 3, tool: 'terminal', preview: 'pytest -x',
    }),
    '⬡ OpenHands subagent · step 3 · terminal · pytest -x'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'observation', step: 3, tool: 'terminal',
      merged_count: 4, preview: '2 passed',
    }),
    '⬡ OpenHands subagent · step 3 · terminal ⇢ observation ×4 · 2 passed'
  );
  assert.equal(
    brain.subagentMarkerText({ ...base, phase: 'message', step: 4, preview: 'All tests pass.' }),
    '⬡ OpenHands subagent · step 4 · All tests pass.'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'awaiting_approval', step: 5, risk: 'MEDIUM', preview: 'rm build/',
    }),
    '⬡ OpenHands subagent · step 5 · ⏸ 等待审批 · risk=MEDIUM · rm build/'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'finished', status: 'finished', steps: 17, duration_ms: 252000,
    }),
    '⬡ OpenHands subagent · ✔ 任务完成 · 17 steps · 4m12s'
  );
  assert.equal(
    brain.subagentMarkerText({
      ...base, phase: 'finished', status: 'stuck', steps: 2, duration_ms: 9000,
    }),
    '⬡ OpenHands subagent · ✗ 任务终止 (stuck) · 2 steps · 9000ms'
  );
});

test('collabMarkerText renders the collab v2 lifecycle markers', () => {
  assert.equal(
    brain.collabMarkerText('COLLAB_TASK_STARTED', {
      collaboration_task_id: 'collab-1',
      mode: 'auto-collab',
      version: 'v2',
      phase: 'EXPLORING',
      experimental: true,
    }),
    '⬢ DragonAI Collab v2 · 任务启动 · 探索阶段(本地, 只读)'
  );
  assert.equal(
    brain.collabMarkerText('COLLAB_REPORT', {
      version: 'v2',
      phase: 'ready',
      revision: 1,
      briefing_ref: 'briefing:1',
      rendered: '## 升级报告\n- 事实: retry 模块缺少上限',
    }),
    [
      '## 升级报告',
      '- 事实: retry 模块缺少上限',
      '',
      '⬢ 升级报告待审 (revision 1) · 回复 **yes** 采用 / **no: <意见>** 修改',
    ].join('\n')
  );
  assert.equal(
    brain.collabMarkerText('COLLAB_DISPATCH', {
      version: 'v2',
      step_number: 3,
      total_steps: 7,
      route: 'subagent',
      subagent_llm: 'qwen3-coder-30b',
      reason: 'multi-file edit with tests',
    }),
    '⬢ 派发 · step 3/7 → OpenHands subagent (模型: qwen3-coder-30b)\n' +
    'why: multi-file edit with tests'
  );
  assert.equal(
    brain.collabMarkerText('COLLAB_REVIEW_DECISION', {
      version: 'v2',
      action: 'REVISE',
      summary: 'edge case missing',
      corrections: ['add a boundary test'],
      final: false,
      corrections_applied: 0,
    }),
    '⬢ 审查 · REVISE · 修复轮（剩 1 次） · edge case missing'
  );
  assert.equal(
    brain.collabMarkerText('COLLAB_TASK_COMPLETED', {
      version: 'v2',
      high_calls: 2,
      local_calls: 5,
      high_input_tokens: 4000,
      estimated_tokens_avoided: 16000,
      token_savings_kind: 'estimated',
    }),
    '⬢ ✔ 协作任务完成 · 审查通过 · high 2 · local 5 · 省 ≈80% (estimated)'
  );
  // Never throws, never breaks the stream: unknown/missing fields skip.
  assert.equal(brain.collabMarkerText('COLLAB_EXPLORATION', null), '');
  assert.equal(brain.collabMarkerText('NOT_A_COLLAB_EVENT', {}), '');
});

test('toolDecisionMarkerText names the executing owner per decision', () => {
  assert.equal(
    brain.toolDecisionMarkerText({ tool: 'grep', decision: 'execute_local' }),
    '◇ Tool call · grep → DragonAI read-only runtime'
  );
  assert.equal(
    brain.toolDecisionMarkerText({
      tool: 'dragonai_subagent:file_edit', decision: 'execute_subagent',
    }),
    '◇ Tool call · dragonai_subagent:file_edit → OpenHands subagent'
  );
  assert.equal(
    brain.toolDecisionMarkerText({
      tool: 'dragonai_stage_fallback', decision: 'execute_local_approved',
    }),
    '◇ Tool call · dragonai_stage_fallback → Codex approval/sandbox'
  );
  assert.equal(
    brain.toolDecisionMarkerText({ tool: 'exec_command', decision: 'allow_codex' }),
    '◇ Tool call · exec_command → Codex approval/sandbox'
  );
  assert.equal(brain.toolDecisionMarkerText({}), '');
});

test('memoryRecallText renders the layered-memory chip', () => {
  const text = brain.memoryRecallText({
    playbooks: 2,
    facts: 5,
    lessons: 1,
    top_playbook: { title: 'edit: fix retry', wins: 4, losses: 1 },
  });
  assert.equal(
    text,
    '◆ Memory recall · 2 playbook(s) · 5 fact(s) · 1 lesson(s) · ' +
    'top="edit: fix retry" (win 4/5)'
  );
  assert.equal(brain.memoryRecallText({ playbooks: 0, facts: 0, lessons: 0 }), '');
});

test('stepUpdateText renders phase-aware step chips', () => {
  assert.equal(
    brain.stepUpdateText({
      phase: 'delegated',
      total: 3,
      step: { n: 2, status: 'in_progress', tool: 'apply_patch', attempts: 2 },
    }),
    '▶ Step 2/3 · apply_patch · attempt 2'
  );
  assert.equal(
    brain.stepUpdateText({
      phase: 'verified',
      total: 3,
      step: { n: 2, status: 'completed' },
    }),
    '✔ Step 2/3 done'
  );
  assert.equal(
    brain.stepUpdateText({
      phase: 'rejected',
      total: 3,
      step: { n: 2, status: 'blocked', note: 'patch did not apply' },
    }),
    '✗ Step 2/3 failed: patch did not apply'
  );
  assert.equal(brain.stepUpdateText({}), '');
});

test('proxiedUsedText renders the proxied-LLM usage chip', () => {
  assert.equal(
    brain.proxiedUsedText({
      purpose: 'collab-review',
      lane: 'copilot',
      model: 'copilot-claude5',
      prompt_tokens: 3000,
      completion_tokens: 200,
      approved_by: 'report-gate',
    }),
    '⇑ Proxied LLM · collab-review · copilot-claude5 · 3.2k tokens (approved: report-gate)'
  );
  // Small spends stay literal; a missing model falls back to the lane.
  assert.equal(
    brain.proxiedUsedText({
      purpose: 'codex-turn',
      lane: 'copilot',
      prompt_tokens: 120,
      completion_tokens: 30,
      approved_by: 'user-phrase',
    }),
    '⇑ Proxied LLM · codex-turn · copilot · 150 tokens (approved: user-phrase)'
  );
  assert.equal(brain.proxiedUsedText(null), '');
});
