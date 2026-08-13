// =============================================================================
// KMTC -> Supabase Sync (Node.js — runs via GitHub Actions)
// Replaces supabase-sync/Code.gs
// =============================================================================

const { createClient } = require('@supabase/supabase-js');

const KMTC_API = process.env.KMTC_API_URL;
const KMTC_KEY = process.env.KMTC_API_KEY;
const sb = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const ALL_DIRS = ['S', 'N', 'E', 'W', 'D', 'P'];

// Minimum spacing between KMTC API calls. The gateway rate-limits
// aggressively; bursts get 429 "Resource usage has been exhausted".
const MIN_INTERVAL_MS = 800;
const MAX_RETRIES = 5;

let lastCallAt = 0;
let rateLimitHits = 0;
let failedFetches = 0;

// ── KMTC API ────────────────────────────────────────────────────────────────

function isRateLimited(body) {
  if (!body || typeof body !== 'object') return false;
  if (String(body.statusCode) === '429') return true;
  const reason = (body.errors || {}).reason || '';
  return /too many requests/i.test(reason);
}

// The gateway answers an unknown voyage with a plain object rather
// than an empty array. That is a valid "no such voyage", not an error.
function isNoData(body) {
  if (!body || typeof body !== 'object') return false;
  return /no data found/i.test(String(body.resultData || ''));
}

function backoffMs(resp, attempt) {
  const ra = parseInt(
    resp.headers.get('retry-after') || '', 10);
  const ms = ra > 0 ? ra * 1000 : 3000 * Math.pow(2, attempt);
  return Math.min(ms, 60000);
}

/**
 * Fetch one voyage. Returns { ok, rows }.
 * ok=false means the API call failed (429/5xx/network) — callers must NOT
 * treat that as "no schedule", or stale data gets deleted or skipped.
 */
async function kmtcFetch(vesselCode, voyageNo) {
  const url = `${KMTC_API}?vesselCode=${
    encodeURIComponent(vesselCode)
  }&voyageNo=${encodeURIComponent(voyageNo)}`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const wait = lastCallAt + MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await sleep(wait);
    lastCallAt = Date.now();

    try {
      const resp = await fetch(url, {
        headers: { 'KMTC-APIKey': KMTC_KEY }
      });

      if (resp.status === 429) {
        rateLimitHits++;
        await sleep(backoffMs(resp, attempt));
        continue;
      }
      if (!resp.ok) {
        failedFetches++;
        console.error(`HTTP ${resp.status} ` +
          `${vesselCode}/${voyageNo}`);
        return { ok: false, rows: [] };
      }

      const body = await resp.json();
      // Non-array body = error envelope, not an empty schedule.
      // The gateway also returns its 429 envelope with HTTP 200,
      // so check the payload before giving up.
      if (!Array.isArray(body)) {
        if (isRateLimited(body)) {
          rateLimitHits++;
          await sleep(backoffMs(resp, attempt));
          continue;
        }
        if (isNoData(body)) return { ok: true, rows: [] };
        failedFetches++;
        console.error(`Bad payload ${vesselCode}/${voyageNo}:`,
          JSON.stringify(body).slice(0, 120));
        return { ok: false, rows: [] };
      }
      return { ok: true, rows: body };
    } catch (e) {
      console.error(`API error ${vesselCode}/${voyageNo}:`,
        e.message);
      await sleep(1000 * (attempt + 1));
    }
  }

  failedFetches++;
  console.error(`GIVE UP after ${MAX_RETRIES} retries: ` +
    `${vesselCode}/${voyageNo}`);
  return { ok: false, rows: [] };
}

function fmtDate(d, t) {
  if (!d || d.length !== 8) return null;
  const s = d.substring(0, 4) + '-' +
    d.substring(4, 6) + '-' + d.substring(6, 8);
  if (t && t.length >= 4) {
    return s + 'T' + t.substring(0, 2) + ':' +
      t.substring(2, 4) + ':00+09:00';
  }
  return s + 'T00:00:00+09:00';
}

function normalizePortCalls(vesselCode, voyageNo,
  dir, raw) {
  return raw.map(p => {
    const arr = p.arrival || {};
    const dep = p.departure || {};
    const bth = p.berthing || {};
    return {
      vessel_code: vesselCode,
      voyage_no: voyageNo,
      direction: p.scheduleDirectionCode || dir,
      port_code: p.portCode || '',
      port_name: (p.portName || '').trim(),
      terminal: ((p.tmnlCode || '') + ' ' +
        (p.tmnlName || '')).trim(),
      eta: fmtDate(arr.arrivalDate, arr.arrivalTime),
      etb: fmtDate(bth.berthingDate,
        bth.berthingTime),
      etd: fmtDate(dep.departureDate,
        dep.departureTime),
      arrival_status: arr.arrivalStatusCode || '',
      departure_status: dep.departureStatusCode || '',
      skip: p.skipYn === 'Y'
    };
  });
}

// ── Supabase Helpers ────────────────────────────────────────────────────────

async function sbGet(table, query) {
  let q = sb.from(table).select('*');
  // Parse simple query params
  if (query) {
    for (const part of query.split('&')) {
      const [field, rest] = part.split('=');
      if (!rest) continue;
      if (rest.startsWith('eq.')) {
        q = q.eq(field, rest.slice(3));
      } else if (rest.startsWith('gte.')) {
        q = q.gte(field, rest.slice(4));
      }
    }
  }
  const { data, error } = await q;
  if (error) {
    console.error('Supabase GET error:', error.message);
    return [];
  }
  return data || [];
}

async function sbPost(table, rows) {
  if (!rows || !rows.length) return true;
  let ok = true;
  // Batch in chunks of 500
  for (let i = 0; i < rows.length; i += 500) {
    const batch = rows.slice(i, i + 500);
    const { error } = await sb.from(table).insert(batch);
    if (error) {
      ok = false;
      console.error('Supabase INSERT error:',
        error.message);
    }
  }
  return ok;
}

async function sbUpsert(table, rows, onConflict) {
  if (!rows || !rows.length) return;
  const { error } = await sb.from(table).upsert(rows, {
    onConflict: onConflict || 'id'
  });
  if (error) {
    console.error('Supabase UPSERT error:',
      error.message);
  }
}

async function sbDelete(table, filters) {
  let q = sb.from(table).delete();
  for (const [field, value] of Object.entries(filters)) {
    q = q.eq(field, value);
  }
  const { error } = await q;
  if (error) {
    console.error('Supabase DELETE error:',
      error.message);
  }
}

// ── Full Fetch (all vessels, ±2.5 years) ────────────────────────────────────

async function initFullFetch() {
  console.log('=== FULL FETCH START ===');
  const { data: ships } = await sb
    .from('ships').select('code');
  if (!ships || !ships.length) {
    console.log('No ships registered.');
    return;
  }

  const now = new Date();
  const yy = now.getFullYear() % 100;
  const prefixes = [];
  for (let y = yy - 2; y <= yy + 2; y++) {
    prefixes.push(y < 10 ? '0' + y : '' + y);
  }

  for (const ship of ships) {
    await fetchSingleVessel(ship.code, prefixes);
    await sleep(100);
  }

  await sbUpsert('config', [{
    key: 'last_update',
    value: new Date().toISOString()
  }], 'key');

  console.log('=== FULL FETCH COMPLETE ===');
}

async function fetchSingleVessel(vesselCode,
  prefixes) {
  if (!prefixes) {
    const yy = new Date().getFullYear() % 100;
    prefixes = [];
    for (let y = yy - 2; y <= yy + 2; y++) {
      prefixes.push(y < 10 ? '0' + y : '' + y);
    }
  }

  const allRows = [];
  let maxSeq = 0, maxPrefix = prefixes[0];
  let incomplete = false;

  for (const pfx of prefixes) {
    let empties = 0, seenData = 0;

    for (let seq = 1; seq <= 50; seq++) {
      let found = false;
      const seqStr = pfx +
        (seq < 10 ? '0' : '') + seq;
      // Sequential, not parallel — 6 concurrent calls trip the
      // gateway rate limit immediately.
      for (const dir of ALL_DIRS) {
        const voy = seqStr + dir;
        const res = await kmtcFetch(vesselCode, voy);
        if (!res.ok) { incomplete = true; continue; }
        if (!res.rows.length) continue;
        found = true;
        allRows.push(...normalizePortCalls(
          vesselCode, voy, dir, res.rows));
      }
      if (found) {
        seenData++;
        empties = 0;
        if (seq > maxSeq || pfx >= maxPrefix) {
          maxSeq = seq;
          maxPrefix = pfx;
        }
      } else {
        if (seenData > 0) {
          empties++;
          if (empties >= 5) break;
        }
      }
    }
  }

  // Never wipe good data on a partial fetch — a rate-limited run
  // would otherwise delete the vessel and re-insert almost nothing.
  if (incomplete || !allRows.length) {
    console.error(`${vesselCode}: fetch incomplete ` +
      `(${allRows.length} rows) — keeping existing data`);
    return 0;
  }

  // Delete existing data for this vessel
  await sbDelete('schedules',
    { vessel_code: vesselCode });

  // Insert all rows
  await sbPost('schedules', allRows);

  // Update voyage cache
  await sbUpsert('voyage_cache', [{
    vessel_code: vesselCode,
    last_prefix: maxPrefix,
    last_seq: maxSeq
  }], 'vessel_code');

  console.log(`${vesselCode}: ${allRows.length} rows,` +
    ` max=${maxPrefix}${maxSeq}`);
  return allRows.length;
}

// ── Incremental Sync (6-hour trigger) ───────────────────────────────────────

async function syncSchedules() {
  console.log('=== INCREMENTAL SYNC START ===');
  const { data: ships } = await sb
    .from('ships').select('code');
  if (!ships || !ships.length) {
    console.log('No ships.');
    return;
  }

  // Get voyage cache
  const { data: cacheRows } = await sb
    .from('voyage_cache').select('*');
  const cache = {};
  (cacheRows || []).forEach(r => {
    cache[r.vessel_code] = {
      prefix: r.last_prefix,
      seq: r.last_seq
    };
  });

  // Get existing schedule keys for dedup
  const { data: existingRows } = await sb
    .from('schedules')
    .select('vessel_code,voyage_no,port_code');
  const existingKeys = new Set();
  (existingRows || []).forEach(r => {
    existingKeys.add(
      `${r.vessel_code}:${r.voyage_no}:${r.port_code}`
    );
  });

  let totalNew = 0, totalUpdated = 0;

  for (const ship of ships) {
    const vc = ship.code;
    const c = cache[vc];

    if (!c) {
      // Never fetched — run full fetch
      console.log(`${vc}: no cache, full fetch.`);
      await fetchSingleVessel(vc);
      continue;
    }

    // ── Step 1: New voyages (last_seq +1~+3) ──
    const newRows = [];
    let maxSeq = c.seq;

    let discoveryFailed = false;

    for (let ns = c.seq + 1; ns <= c.seq + 3; ns++) {
      let seqFound = false;
      for (const dir of ALL_DIRS) {
        const voy = c.prefix +
          (ns < 10 ? '0' : '') + ns + dir;
        const res = await kmtcFetch(vc, voy);
        // A failed call is not proof the voyage is absent —
        // stop advancing instead of recording a false ceiling.
        if (!res.ok) { discoveryFailed = true; break; }
        if (!res.rows.length) continue;
        seqFound = true;
        const rows = normalizePortCalls(
          vc, voy, dir, res.rows);
        for (const r of rows) {
          const key =
            `${r.vessel_code}:${r.voyage_no}:${r.port_code}`;
          if (!existingKeys.has(key)) {
            newRows.push(r);
            existingKeys.add(key);
          }
        }
      }
      if (discoveryFailed) break;
      if (seqFound) maxSeq = ns;
      else break;
    }

    if (newRows.length) {
      await sbPost('schedules', newRows);
      totalNew += newRows.length;
    }
    if (!discoveryFailed && maxSeq > c.seq) {
      await sbUpsert('voyage_cache', [{
        vessel_code: vc,
        last_prefix: c.prefix,
        last_seq: maxSeq
      }], 'vessel_code');
    }

    // ── Step 2: Active + Future status updates ──
    // 30-day lookback to catch currently berthed ships,
    // 90-day horizon ahead — voyages further out are not
    // firm anyway and refreshing them burns the API quota.
    const past30 = new Date(
      Date.now() - 30 * 24 * 3600 * 1000);
    const past30Str = past30.toISOString()
      .split('T')[0];
    const ahead90 = new Date(
      Date.now() + 90 * 24 * 3600 * 1000);
    const ahead90Str = ahead90.toISOString()
      .split('T')[0];

    const { data: futureRows } = await sb
      .from('schedules')
      .select('id,voyage_no,port_code')
      .eq('vessel_code', vc)
      .gte('eta', past30Str)
      .lte('eta', ahead90Str);

    // Group by voyage
    const futureVoys = new Set();
    (futureRows || []).forEach(r => {
      futureVoys.add(r.voyage_no);
    });

    let updatedCount = 0, refreshedVoys = 0, skippedVoys = 0;
    for (const fvoy of futureVoys) {
      const res = await kmtcFetch(vc, fvoy);
      // API failed or returned nothing — keep what we have
      if (!res.ok) { skippedVoys++; continue; }
      if (!res.rows.length) continue;
      const frows = normalizePortCalls(
        vc, fvoy, '', res.rows);
      if (!frows.length) continue;

      // Delete old rows for this voyage
      await sbDelete('schedules', {
        vessel_code: vc,
        voyage_no: fvoy
      });
      // Insert fresh
      const inserted = await sbPost('schedules', frows);
      if (!inserted) {
        console.error(`DATA LOSS RISK ${vc}/${fvoy}: ` +
          `deleted but insert failed`);
      }
      refreshedVoys++;
      updatedCount += frows.length;
    }

    totalUpdated += updatedCount;
    console.log(`${vc}: +${newRows.length} new,` +
      ` ${refreshedVoys}/${futureVoys.size} voys refreshed` +
      (skippedVoys ? `, ${skippedVoys} skipped (API)` : ''));

    await sleep(300);
  }

  // Update config
  await sbUpsert('config', [{
    key: 'last_update',
    value: new Date().toISOString()
  }], 'key');

  console.log(`=== SYNC DONE: +${totalNew} new,` +
    ` ${totalUpdated} updated ===`);
}

function reportApiHealth() {
  console.log(`API: ${rateLimitHits} rate-limit retries,` +
    ` ${failedFetches} failed fetches`);
  if (failedFetches > 0) {
    console.error('WARNING: some voyages could not be ' +
      'fetched — data may be stale. Consider raising ' +
      'MIN_INTERVAL_MS or lowering the cron frequency.');
  }
}

// ── Utilities ───────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── Entry Point ─────────────────────────────────────────────────────────────

async function main() {
  const mode = process.env.SYNC_MODE || 'incremental';
  const vesselCode = process.env.VESSEL_CODE || '';

  console.log(`Mode: ${mode}`);

  if (mode === 'full') {
    await initFullFetch();
  } else if (mode === 'single' && vesselCode) {
    await fetchSingleVessel(vesselCode);
  } else {
    await syncSchedules();
  }

  reportApiHealth();
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
