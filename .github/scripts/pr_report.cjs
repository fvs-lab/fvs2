const fs = require('node:fs');
const path = require('node:path');

const marker = '<!-- fvs-ci-report -->';
const levels = ['critical', 'high', 'medium', 'low', 'info', 'unknown'];
const symbols = {success: '🟢', failure: '🔴', cancelled: '🟡', skipped: '⚪', timed_out: '🔴'};

function text(value) {
  return String(value ?? '').replace(/[\x00-\x1f]/g, ' ').replace(/&/g, '&amp;')
    .replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/@/g, '&#64;')
    .replace(/\|/g, '&#124;').replace(/`/g, '&#96;')
    .replace(/[\[\]!*_\\]/g, char => '&#' + char.charCodeAt(0) + ';').slice(0, 2000);
}

function count(value) {
  return Number.isSafeInteger(value) && value >= 0 && value <= 10000000 ? value : 0;
}

function summaries(directory) {
  if (!fs.existsSync(directory)) return [];
  const root = fs.realpathSync(directory) + path.sep;
  const result = [];
  function visit(current) {
    for (const entry of fs.readdirSync(current, {withFileTypes: true})) {
      const file = path.join(current, entry.name);
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) { visit(file); continue; }
      if (!entry.isFile() || !entry.name.endsWith('.summary.json')) continue;
      if (!fs.realpathSync(file).startsWith(root) || fs.statSync(file).size > 1000000) continue;
      try {
        const data = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (data && ['tests', 'compliance'].includes(data.kind)) result.push(data);
      } catch { /* Missing or invalid reports are displayed as unavailable. */ }
    }
  }
  visit(directory);
  return result;
}

function render(run, jobs, artifacts, reports, repository) {
  const lines = [marker, `CI for ${run.head_sha.slice(0, 12)}: **${text(run.conclusion)}**`, '',
    `[Workflow logs](${run.html_url})`, '', '| Check | Result |', '| --- | --- |'];
  for (const job of jobs) {
    lines.push(`| ${text(job.name)} | ${symbols[job.conclusion] || '🟡'} ${text(job.conclusion || job.status)} |`);
  }
  lines.push('', '| Tests | Passed | Failed | Skipped | Failed packages |', '| --- | ---: | ---: | ---: | ---: |');
  const tests = reports.filter(report => report.kind === 'tests');
  for (const report of tests) {
    const totals = report.counts || {};
    lines.push(`| ${text(report.target)} | ${count(totals.pass)} | ${count(totals.fail)} | ${count(totals.skip)} | ${count(totals.package_fail)} |`);
  }
  if (!tests.length) lines.push('| Reports unavailable | - | - | - | - |');
  const totals = {pass: 0, fail: 0, skip: 0, package_fail: 0};
  for (const report of tests) for (const key of Object.keys(totals)) totals[key] += count(report.counts?.[key]);
  if (Object.values(totals).some(value => value > 0)) {
    lines.push('', '```mermaid', 'pie title Reported test results');
    for (const [key, label] of Object.entries({pass: 'Passed', fail: 'Failed', skip: 'Skipped', package_fail: 'Package failures'})) {
      if (totals[key]) lines.push(`  "${label}" : ${totals[key]}`);
    }
    lines.push('```');
  }
  const scan = reports.find(report => report.kind === 'compliance');
  lines.push('', '**EUProvGuard**');
  if (!scan) {
    lines.push('', 'Report unavailable. Check the compliance job logs.');
  } else {
    lines.push('', scan.ok === true ? '🟢 No findings; live scan completed.' : '🔴 Findings or incomplete scan. This blocks validation and release.');
    const severity = scan.severity || {};
    const total = levels.reduce((sum, level) => sum + count(severity[level]), 0);
    if (total) {
      lines.push('', '```mermaid', 'pie title EUProvGuard findings');
      for (const level of levels) if (count(severity[level])) lines.push(`  "${level}" : ${count(severity[level])}`);
      lines.push('```');
    }
    lines.push('');
    lines.push(`CVE: ${scan.cve == null ? 'unavailable' : count(scan.cve)}; SAST: ${scan.sast == null ? 'unavailable' : count(scan.sast)}`);
    lines.push('');
    for (const error of (Array.isArray(scan.errors) ? scan.errors : []).slice(0, 10)) lines.push(`- ${text(error)}`);
    if (Array.isArray(scan.findings) && scan.findings.length) {
      lines.push('', '<details><summary>Finding details</summary>', '', '| Severity | ID | Description |', '| --- | --- | --- |');
      for (const finding of scan.findings.slice(0, 30)) {
        lines.push(`| ${text(finding.severity)} | ${text(finding.id)} | ${text(finding.description).slice(0, 500)} |`);
      }
      lines.push('', 'Full findings, SBOM and HTML report are in the compliance artifact.', '', '</details>');
    }
  }
  lines.push('', '**Downloads**', '', '| Artifact | Direct download |', '| --- | --- |');
  for (const artifact of artifacts.filter(artifact => !artifact.expired)) {
    const url = `https://github.com/${repository}/actions/runs/${run.id}/artifacts/${artifact.id}`;
    lines.push(`| ${text(artifact.name)} | [Download ZIP](${url}) |`);
  }
  lines.push('', 'Artifact downloads require GitHub sign-in and expire after 30 days. Builds from a failed run are unapproved.',
    'macOS app archives are unsigned; compilation does not verify an FSKit mount.',
    'Windows is currently unsupported: the storage and repository locks require POSIX flock.');
  return lines.join('\n').slice(0, 60000);
}

function currentRun(pr, run) {
  return pr.head.sha === run.head_sha || pr.merge_commit_sha === run.head_sha ||
    (run.pull_requests || []).some(request => request.number === pr.number && request.head?.sha === pr.head.sha);
}

async function publish({github, context, core}) {
  const run = context.payload.workflow_run;
  if (run.event !== 'pull_request') return;
  const repo = context.repo;
  const ownerRepo = `${repo.owner}/${repo.repo}`;
  if (context.payload.repository.full_name !== ownerRepo) throw new Error('Unexpected workflow repository');
  let requests = run.pull_requests || [];
  if (!requests.length) {
    const response = await github.rest.repos.listPullRequestsAssociatedWithCommit({...repo, commit_sha: run.head_sha});
    requests = response.data;
  }
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {...repo, run_id: run.id, per_page: 100});
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {...repo, run_id: run.id, per_page: 100});
  const body = render(run, jobs, artifacts, summaries('fvs-reports'), ownerRepo);
  await core.summary.addRaw(body).write();
  for (const request of requests) {
    const {data: pr} = await github.rest.pulls.get({...repo, pull_number: request.number});
    if (pr.state !== 'open' || pr.base.repo.full_name !== ownerRepo) continue;
    if (!currentRun(pr, run)) continue;
    const comments = await github.paginate(github.rest.issues.listComments, {...repo, issue_number: pr.number, per_page: 100});
    const previous = comments.find(comment => comment.user.login === 'github-actions[bot]' && comment.body?.startsWith(marker));
    if (previous) await github.rest.issues.updateComment({...repo, comment_id: previous.id, body});
    else await github.rest.issues.createComment({...repo, issue_number: pr.number, body});
  }
}

module.exports = {publish, render, text, summaries, currentRun};
