import { and, eq, sql, ne, or, inArray } from "drizzle-orm";
import { agentWakeupRequests, heartbeatRuns, issueRecoveryActions, toolInvocations, type Db } from "@paperclipai/db";
import { readTaskRetryIntent } from "./task-retry.js";

const record = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

/** Generic upstream tools have no common authoritative result-query protocol.
 * Preserve their uncertain mutation fence across recovery. Exact publication
 * protocols can reconcile through their existing owner instead. No transcript
 * assertion or new call/idempotency key clears an old uncertain operation. */
export async function unresolvedRecoveryToolInvocation(db: Db, input: {
  companyId: string; issueId: string; runId: string; connectionId: string;
  toolName: string; risk: string;
}): Promise<string | null> {
  if (input.risk === "read") return null;
  const [prior] = await db.select({ id: toolInvocations.id }).from(toolInvocations)
    .innerJoin(heartbeatRuns, and(eq(heartbeatRuns.id, toolInvocations.runId),
      eq(heartbeatRuns.companyId, input.companyId)))
    .where(and(eq(toolInvocations.companyId, input.companyId), eq(toolInvocations.issueId, input.issueId),
      eq(toolInvocations.connectionId, input.connectionId), eq(toolInvocations.toolName, input.toolName),
      ne(toolInvocations.runId, input.runId),
      sql`${toolInvocations.riskLevel} is distinct from 'read'`,
      inArray(toolInvocations.status, ["executing", "failed", "timed_out"]),
      sql`${toolInvocations.startedAt} is not null`,
      sql`${heartbeatRuns.createdAt} < (select created_at from heartbeat_runs
        where id = ${input.runId} and company_id = ${input.companyId})`,
      sql`(exists (select 1 from ${agentWakeupRequests} w where w.company_id = ${input.companyId}
        and w.run_id = ${input.runId} and w.reason = 'task_retry' and w.status = 'coalesced'
        and w.payload->>'issueId' = ${input.issueId}
        and w.payload#>>'{taskRetry,schema}' = 'paperclip.task-retry.v1')
        or exists (select 1 from ${issueRecoveryActions} a where a.company_id = ${input.companyId}
          and a.source_issue_id = ${input.issueId}
          and a.evidence#>>'{explicitUserContinuation,runId}' = ${input.runId}))`,
    )).limit(1);
  return prior?.id ?? null;
}

/** Keep the mutation fence across provider/session replacement. Read operations
 * and unrelated operations remain available for authoritative reconciliation. */
export async function priorRecoveryApiReceipt(db: Db, input: {
  companyId: string; issueId: string; runId: string; operationId: string; digest: string;
}): Promise<Record<string, unknown> | null> {
  const [run] = await db.select().from(heartbeatRuns).where(and(eq(heartbeatRuns.companyId, input.companyId),
    eq(heartbeatRuns.id, input.runId), sql`coalesce(${heartbeatRuns.nativeIssueId}::text,
      ${heartbeatRuns.issueId}::text, ${heartbeatRuns.contextSnapshot}->>'issueId') = ${input.issueId}`));
  if (!run) return null;
  const [retryWake] = await db.select().from(agentWakeupRequests).where(and(
    eq(agentWakeupRequests.companyId, input.companyId), eq(agentWakeupRequests.runId, input.runId),
    eq(agentWakeupRequests.status, "coalesced"), eq(agentWakeupRequests.reason, "task_retry"),
    sql`${agentWakeupRequests.payload}->>'issueId' = ${input.issueId}`,
  )).limit(1);
  const actions = await db.select({ evidence: issueRecoveryActions.evidence }).from(issueRecoveryActions).where(and(
    eq(issueRecoveryActions.companyId, input.companyId), eq(issueRecoveryActions.sourceIssueId, input.issueId),
    sql`${issueRecoveryActions.evidence}#>>'{explicitUserContinuation,runId}' = ${input.runId}`,
  ));
  const sourceIds = new Set<string>();
  const retry = readTaskRetryIntent(retryWake?.payload);
  if (retry) sourceIds.add(retry.sourceRunId);
  for (const action of actions) {
    const source = action.evidence.runId;
    if (typeof source === "string") sourceIds.add(source);
  }
  if (!sourceIds.size) return null;
  const sources = await db.select({ result: heartbeatRuns.resultJson }).from(heartbeatRuns).where(and(
    eq(heartbeatRuns.companyId, input.companyId),
    sql`coalesce(${heartbeatRuns.nativeIssueId}::text, ${heartbeatRuns.issueId}::text,
      ${heartbeatRuns.contextSnapshot}->>'issueId') = ${input.issueId}`,
    ne(heartbeatRuns.id, input.runId),
    or(inArray(heartbeatRuns.id, [...sourceIds]),
      sql`${heartbeatRuns.createdAt} < (select created_at from heartbeat_runs where id = ${input.runId})`)));
  let completed: Record<string, unknown> | null = null;
  for (const source of sources) {
    for (const receipt of Object.values(record(source.result?.apiToolReceipts)).map(record)) {
      if (receipt.operationId !== input.operationId) continue;
      const result = record(receipt.result);
      if (receipt.state !== "completed" || result.outcome === "unknown" || result.error === "api_outcome_unknown") {
        return { state: "pending", result: { ok: false, status: null, error: "api_outcome_unknown",
          outcome: "unknown", guidance: "Reconcile the existing operation before repeating it. Independent work and reads remain available." } };
      }
      if (receipt.digest === input.digest) completed = receipt;
    }
  }
  return completed;
}
