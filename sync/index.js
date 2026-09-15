// =============================================================================
// KMTC -> Supabase Sync (Node.js — runs via GitHub Actions)
// Replaces supabase-sync/Code.gs
// =============================================================================

const { createClient } = require('@supabase/supabase-js');
const { syncRoutes } = require('./routes.js');

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
        } else if (seq >= 5) {
          // Voyage numbers restart at 01 each year, so a prefix with
          // nothing in 01..05 has no voyages at all — skip the rest.
          break;
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

// ── Incremental Sync ────────────────────────────────────────────────────────

/**
 * opts.discover   probe for voyages past the cached sequence
 * opts.aheadDays  how far ahead to refresh existing voyages
 * opts.fromDays   refresh only voyages starting at least this far ahead
 *                 (weekly long-range sweep). Without it the run refreshes
 *                 the active window: every voyage with a call that has not
 *                 departed yet (3-day grace so actuals settle). Completed
 *                 voyages never change, so re-fetching them wastes quota.
 *
 * The gateway meters total request volume, so the frequent run keeps
 * the horizon short and the wide sweeps run a few times a day.
 */
async function syncSchedules(opts) {
  const discover = opts.discover;
  const aheadDays = opts.aheadDays;
  const fromDays = opts.fromDays || 0;
  console.log(`=== SYNC START (discover=${discover},` +
    ` from=${fromDays}d ahead=${aheadDays}d) ===`);
  let { data: ships } = await sb
    .from('ships').select('code');
  // opts.vessels: comma-separated codes to limit a manual run to
  if (opts.vessels) {
    const want = opts.vessels.split(',').map(s => s.trim());
    ships = (ships || []).filter(s => want.includes(s.code));
  }
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

  // Get existing schedule keys for dedup. PostgREST caps a plain
  // select at 1000 rows, so page through the table explicitly.
  const existingKeys = new Set();
  if (discover) {
    const PAGE = 1000;
    for (let from = 0; ; from += PAGE) {
      const { data: page } = await sb
        .from('schedules')
        .select('vessel_code,voyage_no,port_code')
        .order('id', { ascending: true })
        .range(from, from + PAGE - 1);
      if (!page || !page.length) break;
      page.forEach(r => {
        existingKeys.add(
          `${r.vessel_code}:${r.voyage_no}:${r.port_code}`
        );
      });
      if (page.length < PAGE) break;
    }
    console.log(`dedup keys: ${existingKeys.size}`);
  }

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

    for (let ns = c.seq + 1; discover && ns <= c.seq + 3; ns++) {
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
    // The horizon ahead is short on the frequent run because
    // distant voyages are not firm and cost API quota.
    const dayStr = offsetDays =>
      new Date(Date.now() + offsetDays * 24 * 3600 * 1000)
        .toISOString().split('T')[0];
    const aheadStr = dayStr(aheadDays);

    // Nearest voyages first, so a run cut off by the job timeout
    // leaves only the far tail stale.
    let fq = sb
      .from('schedules')
      .select('id,voyage_no,port_code')
      .eq('vessel_code', vc)
      .lte('eta', aheadStr)
      .order('eta', { ascending: true });
    if (fromDays) {
      fq = fq.gte('eta', dayStr(fromDays));
    } else {
      // Any call not departed yet keeps its voyage in the window,
      // including ships sitting in port or dock past their ETD.
      fq = fq.gte('etd', dayStr(-3));
    }
    const { data: futureRows } = await fq;

    // Group by voyage
    const futureVoys = new Set();
    (futureRows || []).forEach(r => {
      futureVoys.add(r.voyage_no);
    });

    let updatedCount = 0, refreshedVoys = 0, skippedVoys = 0;
    let liveVoys = 0;
    const ghostCandidates = [];
    for (const fvoy of futureVoys) {
      const res = await kmtcFetch(vc, fvoy);
      // API failed — keep what we have
      if (!res.ok) { skippedVoys++; continue; }
      // Explicit "no such voyage": the gateway withdrew or renumbered
      // it. Decide after the loop, once we know the gateway answered
      // properly for this vessel at all.
      if (!res.rows.length) { ghostCandidates.push(fvoy); continue; }
      liveVoys++;
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

    // ── Step 3: Drop ghost voyages ──
    // Only when the gateway proved responsive for this vessel, and only
    // voyages with no past calls — a withdrawn future proforma, not
    // history. Lowering the cache lets discovery pick the number up
    // again if KMTC republishes it.
    let purgedVoys = 0;
    if (ghostCandidates.length &&
        (liveVoys > 0 || await gatewayKnowsVessel(vc))) {
      for (const gvoy of ghostCandidates) {
        if (!(await isGhostVoyage(vc, gvoy))) continue;
        await sbDelete('schedules', {
          vessel_code: vc, voyage_no: gvoy
        });
        purgedVoys++;
        console.log(`${vc}/${gvoy}: GHOST purged (gateway has no data)`);
      }
      if (purgedVoys) await lowerVoyageCache(vc, c.prefix, c.seq);
    }

    totalUpdated += updatedCount;
    console.log(`${vc}: +${newRows.length} new,` +
      ` ${refreshedVoys}/${futureVoys.size} voys refreshed` +
      (skippedVoys ? `, ${skippedVoys} skipped (API)` : '') +
      (purgedVoys ? `, ${purgedVoys} ghost purged` : ''));

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

/**
 * A voyage is a ghost when the gateway answers "no data" twice in a row
 * and none of its stored calls lie in the past. One empty answer could be
 * a hiccup; a voyage with actual calls is history we keep regardless.
 */
async function isGhostVoyage(vesselCode, voyageNo) {
  await sleep(5000);
  const again = await kmtcFetch(vesselCode, voyageNo);
  if (!again.ok || again.rows.length) return false;

  const todayStr = new Date().toISOString().split('T')[0];
  const { data, error } = await sb
    .from('schedules')
    .select('id')
    .eq('vessel_code', vesselCode)
    .eq('voyage_no', voyageNo)
    .lt('eta', todayStr)
    .limit(1);
  if (error) return false;
  return !(data && data.length);
}

/**
 * The long-range sweep can hit a vessel whose whole far tail is ghosts,
 * so it sees no live voyage to prove the gateway answers for that vessel.
 * Fetch its nearest upcoming voyage as the health check instead.
 */
async function gatewayKnowsVessel(vesselCode) {
  const recentStr = new Date(Date.now() - 14 * 24 * 3600 * 1000)
    .toISOString().split('T')[0];
  const { data } = await sb
    .from('schedules')
    .select('voyage_no')
    .eq('vessel_code', vesselCode)
    .gte('eta', recentStr)
    .order('eta', { ascending: true })
    .limit(1);
  if (!data || !data.length) return false;
  const res = await kmtcFetch(vesselCode, data[0].voyage_no);
  return res.ok && res.rows.length > 0;
}

/**
 * After purging, the highest voyage still stored may sit below the cached
 * sequence. Discovery probes from the cache, so pull it down or the
 * republished voyage would never be found.
 */
async function lowerVoyageCache(vesselCode, prefix, cachedSeq) {
  const { data } = await sb
    .from('schedules')
    .select('voyage_no')
    .eq('vessel_code', vesselCode)
    .order('voyage_no', { ascending: false })
    .limit(200);
  let maxSeq = 0;
  (data || []).forEach(r => {
    const v = String(r.voyage_no || '');
    if (!v.startsWith(prefix)) return;
    const n = parseInt(v.slice(prefix.length, prefix.length + 2), 10);
    if (n > maxSeq) maxSeq = n;
  });
  if (!maxSeq || maxSeq >= cachedSeq) return;
  await sbUpsert('voyage_cache', [{
    vessel_code: vesselCode,
    last_prefix: prefix,
    last_seq: maxSeq
  }], 'vessel_code');
  console.log(`${vesselCode}: voyage cache ${cachedSeq} -> ${maxSeq}`);
}

function reportApiHealth() {
  console.log(`API: ${rateLimitHits} rate-limit retries,` +
    ` ${failedFetches} failed fetches`);
  if (failedFetches > 0) {
    console.error('WARNING: some voyages could not be ' +
      'fetched — data may be stale. The gateway meters total ' +
      'volume, so cut calls (shorter horizon, fewer runs) ' +
      'rather than slowing them down.');
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
    await syncRoutes(sb, {});
  } else if (mode === 'single' && vesselCode) {
    await fetchSingleVessel(vesselCode);
  } else if (mode === 'daily') {
    // Once a day: look for new voyages and refresh the wide horizon
    await syncSchedules({ discover: true, aheadDays: 90 });
    await syncRoutes(sb, {});
  } else if (mode === 'wide') {
    // Midday and evening: same sweep as daily, minus the route sync.
    // KMTC revises schedules during office hours; one sweep at dawn
    // left those changes invisible until the next morning.
    await syncSchedules({
      discover: true, aheadDays: 90, vessels: vesselCode });
  } else if (mode === 'longrange') {
    // Weekly: proforma voyages beyond the daily horizon. They drift by
    // weeks otherwise, since nothing else touches them until they come
    // within 90 days.
    await syncSchedules({
      discover: false, fromDays: 90, aheadDays: 400, vessels: vesselCode });
  } else if (mode === 'routes') {
    await syncRoutes(sb, {});
  } else if (mode === 'routes-backfill') {
    // Whole archive in one pass. It needs ~3,300 leg searches, which at the
    // module's 600ms spacing is roughly 35 minutes — inside the job timeout
    // and gentler on ekmtc.com than several truncated passes would be.
    // Neighbour labelling is off: over years of history a vessel changes
    // service repeatedly, so guessing from an adjacent voyage is worse than
    // leaving the call unlabelled.
    await syncRoutes(sb, {
      backDays: 1200, aheadDays: 700,
      maxCalls: 4500, allowNeighbour: false, prune: true
    });
  } else {
    // Frequent run: only the voyages crews actually look at
    await syncSchedules({ discover: false, aheadDays: 30 });
  }

  reportApiHealth();
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
