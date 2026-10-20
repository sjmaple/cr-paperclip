import { createHmac, randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { issueRecoveryActions, type Db } from "@paperclipai/db";
import { getCloudStackContext, type CloudInstanceEnv } from "./cloud-instance.js";
import { readSystemRecovery } from "./system-recovery-state.js";
import { logger } from "../middleware/logger.js";

/** Separate outbox fields cannot be clobbered by a late recovery phase write.
 * The source action remains the protected, system-owned incident on self-hosts. */
export async function deliverSystemRecoveryIncidents(db: Db, options: {
  env?: CloudInstanceEnv; fetchImpl?: typeof fetch; now?: number;
} = {}) {
  const env = options.env ?? process.env;
  const cloud = getCloudStackContext(env);
  const secret = env.BETTER_AUTH_SECRET ?? env.PAPERCLIP_AUTH_SECRET;
  if (!cloud?.primaryHost || !cloud.cloudOrigin || !secret) return;
  const origin = new URL(cloud.cloudOrigin);
  if (origin.protocol !== "https:" || origin.username || origin.password || origin.pathname !== "/") return;
  const host = cloud.primaryHost.toLowerCase();
  const token = createHmac("sha256", secret).update(`paperclip-cloud-recovery-incidents-v1\n${host}`).digest("hex");
  const now = options.now ?? Date.now();
  const rows = await db.select().from(issueRecoveryActions).where(and(
    sql`${issueRecoveryActions.evidence}#>'{systemRecovery,incident}' is not null`,
    sql`${issueRecoveryActions.evidence}#>'{systemRecovery,incident}' <> 'null'::jsonb`,
    sql`(${issueRecoveryActions.evidence}#>'{recoveryIncidentDelivery,revision}' is distinct from ${issueRecoveryActions.evidence}#>'{systemRecovery,revision}')`,
    sql`coalesce((${issueRecoveryActions.evidence}#>>'{recoveryIncidentDelivery,nextAttemptAt}')::bigint, 0) <= ${now}`,
    sql`coalesce((${issueRecoveryActions.evidence}#>>'{recoveryIncidentDelivery,expiresAt}')::bigint, 0) <= ${now}`,
  )).orderBy(issueRecoveryActions.updatedAt).limit(5);
  for (const action of rows) {
    const state = readSystemRecovery(action.evidence);
    if (!state?.incident || !state.failure) continue;
    const claimId = randomUUID();
    const previous = action.evidence.recoveryIncidentDelivery as { attempts?: number } | undefined;
    const attempts = Math.min((previous?.attempts ?? 0) + 1, 10);
    const claim = { claimId, attempts, expiresAt: now + 60_000, nextAttemptAt: now + 60_000 };
    const [claimed] = await db.update(issueRecoveryActions).set({ evidence: sql`jsonb_set(${issueRecoveryActions.evidence},
      '{recoveryIncidentDelivery}', ${JSON.stringify(claim)}::jsonb)` }).where(and(
        eq(issueRecoveryActions.id, action.id), eq(issueRecoveryActions.companyId, action.companyId),
        sql`${issueRecoveryActions.evidence}#>>'{systemRecovery,revision}' = ${String(state.revision)}`,
        sql`coalesce(${issueRecoveryActions.evidence}->'recoveryIncidentDelivery', 'null'::jsonb) is not distinct from ${JSON.stringify(action.evidence.recoveryIncidentDelivery ?? null)}::jsonb`,
      )).returning({ id: issueRecoveryActions.id });
    if (!claimed) continue;
    let acknowledged = false;
    try {
      const response = await (options.fetchImpl ?? fetch)(new URL("/v1/tenant/recovery-incidents", origin), {
        method: "POST", redirect: "error", signal: AbortSignal.timeout(10_000),
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json", "x-paperclip-cloud-tenant-host": host },
        body: JSON.stringify({ schema: "paperclip.recovery-incident.v1", sourceRecoveryId: action.id,
          companyId: action.companyId, issueId: action.sourceIssueId, sourceRunId: state.sourceRunId,
          revision: state.revision, state: state.incident.status === "resolved" ? "resolved" : "open",
          phase: state.phase, failureCode: state.failure.code, evidenceFingerprint: state.failure.evidenceFingerprint,
          occurredAt: action.updatedAt.toISOString() }),
      });
      const receipt = response.ok ? await response.json() as { revision?: unknown } : null;
      acknowledged = typeof receipt?.revision === "number" && receipt.revision >= state.revision;
    } catch { /* The durable outbox owns retry; never block preservation or dispatch. */ }
    const result = { claimId: null, expiresAt: 0, attempts: acknowledged ? 0 : attempts,
      ...(acknowledged ? { revision: state.revision } : {}),
      nextAttemptAt: acknowledged ? 0 : now + Math.min(10 * 60_000, 30_000 * 2 ** (attempts - 1)) };
    await db.update(issueRecoveryActions).set({ evidence: sql`jsonb_set(${issueRecoveryActions.evidence},
      '{recoveryIncidentDelivery}', ${JSON.stringify(result)}::jsonb)` }).where(and(
        eq(issueRecoveryActions.companyId, action.companyId), eq(issueRecoveryActions.id, action.id),
        sql`${issueRecoveryActions.evidence}#>>'{recoveryIncidentDelivery,claimId}' = ${claimId}`));
    if (!acknowledged) logger.warn({ recoveryActionId: action.id, revision: state.revision }, "Recovery incident delivery deferred");
  }
}
