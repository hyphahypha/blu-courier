/**
 * BLU Courier — pushes each dealer's vAuto photo-manifest CSV to their FTP.
 *
 * vAuto polls a manifest CSV over FTP (it can't read folders or HTTPS). The
 * BLU Studio Worker BUILDS that CSV on demand at /feed/{slug}.csv, but a
 * Cloudflare Worker can't make an outbound FTP connection — so this tiny job
 * does the FTP leg. It runs on a schedule (GitHub Actions cron) so the whole
 * pipeline is hands-off: employees upload in the PWA, the Worker updates the
 * feed, and this courier delivers it to vAuto a few minutes later.
 *
 * Flow:
 *   1. GET {WORKER_BASE}/admin/dealers   (admin-token) → every dealer + its FTP creds
 *   2. For each dealer that has FTP configured:
 *        GET {WORKER_BASE}/feed/{slug}.csv   → the manifest text
 *        FTP upload it as {slug}_photos.csv (or ftp.filename) to ftp.path
 *
 * Single source of truth = the Worker's KV registry. Onboarding a dealer in the
 * CRM (or via the admin API) makes the courier pick it up automatically with no
 * change here. The ONLY secret this job needs is STUDIO_ADMIN_TOKEN.
 */

import { Client } from "basic-ftp";
import { Readable } from "node:stream";

const WORKER_BASE = (process.env.WORKER_BASE || "https://blu-studio.ali-855.workers.dev").replace(/\/$/, "");
const ADMIN_TOKEN = process.env.STUDIO_ADMIN_TOKEN;

if (!ADMIN_TOKEN) {
  console.error("FATAL: STUDIO_ADMIN_TOKEN not set.");
  process.exit(1);
}

async function getDealers() {
  const res = await fetch(`${WORKER_BASE}/admin/dealers`, {
    headers: { Authorization: `Bearer ${ADMIN_TOKEN}` }
  });
  if (!res.ok) throw new Error(`admin/dealers → HTTP ${res.status}`);
  const data = await res.json();
  if (!data.ok) throw new Error(`admin/dealers → ${data.error}`);
  return data.dealers || [];
}

async function getFeedCsv(slug) {
  // Feed is auth-gated (business-confidential VIN list). The courier holds the
  // admin token, so it's authorized to read every dealer's feed.
  const res = await fetch(`${WORKER_BASE}/feed/${slug}.csv`, {
    headers: { Authorization: `Bearer ${ADMIN_TOKEN}` }
  });
  if (!res.ok) throw new Error(`feed/${slug}.csv → HTTP ${res.status}`);
  return await res.text();
}

async function pushOne(dealer) {
  const { slug, ftp } = dealer;
  if (!ftp || !ftp.host || !ftp.user) {
    return { slug, skipped: true, reason: "no ftp config" };
  }
  const csv = await getFeedCsv(slug);
  const rowCount = Math.max(0, csv.trim().split(/\r?\n/).length - 1); // minus header
  const filename = ftp.filename || `${slug}_photos.csv`;
  const remotePath = (ftp.path || "").replace(/\/$/, "");
  const remote = remotePath ? `${remotePath}/${filename}` : filename;

  const client = new Client(30_000);
  client.ftp.verbose = false;
  try {
    await client.access({
      host: ftp.host,
      port: ftp.port || 21,
      user: ftp.user,
      password: ftp.password,
      secure: !!ftp.secure
    });
    if (remotePath) { try { await client.ensureDir(remotePath); } catch {} }
    // Upload from an in-memory stream — no temp file needed.
    await client.uploadFrom(Readable.from([csv]), remote);
    return { slug, ok: true, rows: rowCount, remote };
  } finally {
    client.close();
  }
}

async function main() {
  const startedAt = new Date().toISOString();
  console.log(`[courier] start ${startedAt} · worker ${WORKER_BASE}`);
  let dealers;
  try {
    dealers = await getDealers();
  } catch (e) {
    console.error(`[courier] could not list dealers: ${e.message}`);
    process.exit(1);
  }
  const withFtp = dealers.filter((d) => d.ftp && d.ftp.host);
  console.log(`[courier] ${dealers.length} dealers, ${withFtp.length} with FTP configured`);

  let ok = 0, failed = 0, skipped = 0;
  for (const dealer of dealers) {
    try {
      const r = await pushOne(dealer);
      if (r.skipped) { skipped++; console.log(`[courier] · ${r.slug}: skipped (${r.reason})`); }
      else { ok++; console.log(`[courier] ✓ ${r.slug}: ${r.rows} vehicles → ${r.remote}`); }
    } catch (e) {
      failed++;
      console.error(`[courier] ✗ ${dealer.slug}: ${e.message}`);
    }
  }
  console.log(`[courier] done · ${ok} pushed, ${failed} failed, ${skipped} skipped`);
  // Fail the job only if a configured dealer failed — so the run goes red in
  // GitHub and you get notified, but skips (no FTP yet) are not errors.
  if (failed > 0) process.exit(1);
}

main().catch((e) => { console.error("[courier] fatal", e); process.exit(1); });
