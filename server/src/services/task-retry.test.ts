import { randomUUID } from "node:crypto";
import { and, eq } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { agents, agentWakeupRequests, companies, createDb, heartbeatRuns, issues, issueRecoveryActions } from "@paperclipai/db";
import { startEmbeddedPostgresTestDatabase } from "../__tests__/helpers/embedded-postgres.js";
import { requestTaskRetry } from "./task-retry.js";
import { readFreshWorkspaceReadiness } from "./fresh-workspace-continuation.js";
import { settleUnrecoverableExecutions } from "./execution-recovery-resolution.js";
import { createSystemRecovery, failSystemRecovery, readSystemRecovery, SystemRecoveryFailure } from "./system-recovery-state.js";

describe("durable task Retry admission", () => {
  let postgres: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    postgres = await startEmbeddedPostgresTestDatabase("task-retry-");
    db = createDb(postgres.connectionString);
  }, 30_000);
  afterAll(async () => { await db?.$client.end(); await postgres?.cleanup(); });
  async function seed() {
    const companyId = randomUUID(), issueId = randomUUID(), agentId = randomUUID(), runId = randomUUID();
    await db.insert(companies).values({ id: companyId, name: "Retry company", issuePrefix: `T${companyId.slice(0, 6)}` });
    await db.insert(agents).values({ id: agentId, companyId, name: "Worker", adapterType: "paperclip_runner", status: "idle" });
    await db.insert(issues).values({ id: issueId, companyId, title: "Continue saved work", status: "blocked", assigneeAgentId: agentId });
    await db.insert(heartbeatRuns).values({ id: runId, companyId, agentId, issueId, status: "failed",
      contextSnapshot: { issueId }, finishedAt: new Date() });
    return { db, companyId, issueId, actorId: "board", observedRunId: runId, idempotencyKey: randomUUID() };
  }
  it("serializes concurrent clicks and remembers each key after completion", async () => {
    const input = await seed();
    const second = { ...input, idempotencyKey: randomUUID() };
    const [one, two] = await Promise.all([requestTaskRetry(input), requestTaskRetry(second)]);
    expect(one.requestId).toBe(two.requestId);
    const rows = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, input.companyId));
    expect(rows.filter(row => row.status === "deferred_issue_execution")).toHaveLength(1);
    expect(rows).toHaveLength(2);
    await db.update(agentWakeupRequests).set({ status: "failed" }).where(eq(agentWakeupRequests.id, one.requestId!));
    expect((await requestTaskRetry(input)).requestId).toBe(one.requestId);
    expect((await requestTaskRetry(second)).requestId).toBe(one.requestId);
    const next = await requestTaskRetry({ ...input, idempotencyKey: randomUUID() });
    expect(next.requestId).not.toBe(one.requestId);
  });
  it("does not resurrect completed tasks or retry an obsolete assignment", async () => {
    const input = await seed();
    await db.update(issues).set({ status: "done" }).where(eq(issues.id, input.issueId));
    expect(await requestTaskRetry(input)).toMatchObject({ accepted: false, stale: true });
    await db.update(issues).set({ status: "blocked", assigneeAgentId: null }).where(eq(issues.id, input.issueId));
    expect(await requestTaskRetry(input)).toMatchObject({ accepted: false, stale: true });
    expect(await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, input.companyId))).toHaveLength(0);
  });
  it("replaces an exhausted unadmitted generation only on a new deliberate click", async () => {
    const input = await seed();
    const first = await requestTaskRetry(input);
    const state = failSystemRecovery(createSystemRecovery(input.observedRunId, first.requestId!),
      new SystemRecoveryFailure("source_unavailable", "service_required"));
    const [action] = await db.insert(issueRecoveryActions).values({
      companyId: input.companyId, sourceIssueId: input.issueId, kind: "active_run_watchdog",
      ownerType: "system", cause: "legacy_execution_requires_reconciliation", fingerprint: randomUUID(),
      nextAction: "System recovery", evidence: { runId: input.observedRunId, systemRecovery: state },
    }).returning();
    expect((await requestTaskRetry(input)).requestId).toBe(first.requestId);
    const nextInput = { ...input, idempotencyKey: randomUUID() };
    const next = await requestTaskRetry(nextInput);
    expect(next.requestId).not.toBe(first.requestId);
    const [updated] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, action.id));
    expect(readSystemRecovery(updated.evidence)).toMatchObject({ generationId: next.requestId, attempts: 0 });
    expect((await requestTaskRetry(nextInput)).requestId).toBe(next.requestId);
    const wakes = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.companyId, input.companyId));
    expect(wakes.find(row => row.id === first.requestId)?.status).toBe("failed");
    expect(wakes.filter(row => row.status === "deferred_issue_execution")).toHaveLength(1);
  });
  it("binds a key to its company, task and actor across all statuses", async () => {
    const input = await seed();
    const receipt = await requestTaskRetry(input);
    await db.update(agentWakeupRequests).set({ status: "completed" }).where(and(
      eq(agentWakeupRequests.companyId, input.companyId), eq(agentWakeupRequests.id, receipt.requestId!)));
    await expect(requestTaskRetry({ ...input, actorId: "another-user" })).rejects.toThrow("different task or user");
    expect((await requestTaskRetry(input)).requestId).toBe(receipt.requestId);
    const other = await seed();
    expect((await requestTaskRetry({ ...other, idempotencyKey: input.idempotencyKey })).requestId).not.toBe(receipt.requestId);
  });
  it("keeps an adopted historical hold with its maintenance owner", async () => {
    const input = await seed();
    const system = createSystemRecovery(input.observedRunId);
    const [action] = await db.insert(issueRecoveryActions).values({
      companyId: input.companyId, sourceIssueId: input.issueId, kind: "active_run_watchdog",
      status: "active", ownerType: "system", returnOwnerAgentId: null,
      cause: "legacy_execution_requires_reconciliation", fingerprint: randomUUID(),
      nextAction: "System preservation", evidence: { runId: input.observedRunId, systemRecovery: system },
    }).returning();
    await settleUnrecoverableExecutions(db);
    const [current] = await db.select().from(issueRecoveryActions).where(eq(issueRecoveryActions.id, action.id));
    expect(current.status).toBe("active");
    expect(current.ownerType).toBe("system");
    expect(current.evidence.systemRecovery).toEqual(system);
    expect(current.evidence.automaticRecovery).toBeUndefined();
  });
  it.each(["evaluate", "continue"])("keeps an admitted %s retry isolated after a failure without a preserved workspace", async strategy => {
    const input = await seed();
    const receipt = await requestTaskRetry(input);
    const [source] = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, input.observedRunId));
    const [successor] = await db.insert(heartbeatRuns).values({ companyId: input.companyId,
      agentId: source.agentId, issueId: input.issueId, status: "queued",
      contextSnapshot: { issueId: input.issueId, taskRetryRequestId: receipt.requestId } }).returning();
    expect(await readFreshWorkspaceReadiness(db, successor)).toBeNull();
    const [wake] = await db.select().from(agentWakeupRequests).where(eq(agentWakeupRequests.id, receipt.requestId!));
    await db.update(agentWakeupRequests).set({ runId: successor.id, status: "coalesced",
      payload: { ...wake.payload, taskRetry: { ...(wake.payload!.taskRetry as object), strategy } } })
      .where(eq(agentWakeupRequests.id, receipt.requestId!));
    const readiness = await readFreshWorkspaceReadiness(db, successor);
    expect(readiness).toMatchObject({ mode: "fresh_isolated_workspace", retryIntentId: receipt.requestId,
      sourceRunId: source.id });
    expect(await readFreshWorkspaceReadiness(db, successor)).toEqual(readiness);
    expect(await readFreshWorkspaceReadiness(db, { ...successor, id: randomUUID() })).toBeNull();
  });
});
