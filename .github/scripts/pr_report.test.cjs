const assert = require('node:assert/strict');
const test = require('node:test');
const {render, text, currentRun} = require('./pr_report.cjs');

const run = {id: 1, head_sha: '0123456789abcdef', conclusion: 'failure', html_url: 'https://github.com/fvs-lab/core/actions/runs/1'};

test('missing reports never claim a clean scan', () => {
  const result = render(run, [], [], [], 'fvs-lab/core');
  assert.match(result, /Report unavailable/);
  assert.doesNotMatch(result, /No findings/);
});

test('failed scans retain charts and official artifact download links', () => {
  const result = render(run, [{name: 'Compliance', conclusion: 'failure'}], [{id: 2, name: 'build-linux-amd64'}],
    [{kind: 'compliance', ok: false, severity: {critical: 2}, findings: [], errors: ['findings']}], 'fvs-lab/core');
  assert.match(result, /"critical" : 2/);
  assert.match(result, /actions\/runs\/1\/artifacts\/2/);
  assert.match(result, /blocks validation and release/);
});

test('report text cannot inject HTML, mentions or table cells', () => {
  const result = text('<img src=x> @everyone | `code`\nnew');
  assert.doesNotMatch(result, /<|>|@|\||`|\n/);
});

test('stale runs cannot overwrite the current PR report', () => {
  const pr = {number: 3, head: {sha: 'new'}, merge_commit_sha: 'new-merge'};
  assert.equal(currentRun(pr, {head_sha: 'old', pull_requests: []}), false);
  assert.equal(currentRun(pr, {head_sha: 'old', pull_requests: [{number: 3, head: {sha: 'old'}}]}), false);
  assert.equal(currentRun(pr, {head_sha: 'new'}), true);
  assert.equal(currentRun(pr, {head_sha: 'new-merge'}), true);
  assert.equal(currentRun(pr, {head_sha: 'snapshot-merge', pull_requests: [{number: 3, head: {sha: 'new'}}]}), false);
});
