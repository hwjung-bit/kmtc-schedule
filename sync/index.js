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

// ── KMTC API ────────────────────────────────────────────────────────────────

async function kmtcFetch(vesselCode, voyageNo) {
  try {
    const url = `${KMTC_API}?vesselCode=${
      encodeURIComponent(vesselCode)
    }&voyageNo=${encodeURIComponent(voyageNo)}`;
    const resp = await fetch(url, {
      headers: { 'KMTC-APIKey': KMTC_KEY }
    });
    if (!resp.ok) return [];
    const body = await resp.json();
    return Array.isArray(body) ? body : [];
  } catch (e) {
    console.error(`API error ${vesselCode}/${voyageNo}:`,
      e.message);
    return [];
  }
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
    return {
      vessel_code: vesselCode,
      voyage_no: voyageNo,
      direction: p.scheduleDirectionCode || dir,
      port_code: p.portCode || '',
      port_name: (p.portName || '').trim(),
      terminal: ((p.tmnlCode || '') + ' ' +
        (p.tmnlName || '')).trim(),
      eta: fmtDate(arr.arrivalDate, arr.arrivalTime),
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
  if (!rows || !rows.length) return;
  // Batch in chunks of 500
  for (let i = 0; i < rows.length; i += 500) {
    const batch = rows.slice(i, i + 500);
    const { error } = await sb.from(table).insert(batch);
    if (error) {
      console.error('Supabase INSERT error:',
        error.message);
    }
  }
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
    // Small delay between vessels
    await sleep(300);
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

  for (const pfx of prefixes) {
    let empties = 0, seenData = 0;

    for (let seq = 1; seq <= 50; seq++) {
      let found = false;
      for (const dir of ALL_DIRS) {
        const voy = pfx +
          (seq < 10 ? '0' : '') + seq + dir;
        const raw = await kmtcFetch(vesselCode, voy);
        if (!raw.length) continue;
        found = true;
        const rows = normalizePortCalls(
          vesselCode, voy, dir, raw);
        allRows.push(...rows);
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

  // Delete existing data for this vessel
  await sbDelete('schedules',
    { vessel_code: vesselCode });

  // Insert all rows
  if (allRows.length) await sbPost('schedules', allRows);

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

    for (let ns = c.seq + 1; ns <= c.seq + 3; ns++) {
      let seqFound = false;
      for (const dir of ALL_DIRS) {
        const voy = c.prefix +
          (ns < 10 ? '0' : '') + ns + dir;
        const raw = await kmtcFetch(vc, voy);
        if (!raw.length) continue;
        seqFound = true;
        const rows = normalizePortCalls(
          vc, voy, dir, raw);
        for (const r of rows) {
          const key =
            `${r.vessel_code}:${r.voyage_no}:${r.port_code}`;
          if (!existingKeys.has(key)) {
            newRows.push(r);
            existingKeys.add(key);
          }
        }
      }
      if (seqFound) maxSeq = ns;
      else break;
    }

    if (newRows.length) {
      await sbPost('schedules', newRows);
      totalNew += newRows.length;
    }
    if (maxSeq > c.seq) {
      await sbUpsert('voyage_cache', [{
        vessel_code: vc,
        last_prefix: c.prefix,
        last_seq: maxSeq
      }], 'vessel_code');
    }

    // ── Step 2: Active + Future status updates ──
    // 30-day lookback to catch currently berthed ships
    const past30 = new Date(
      Date.now() - 30 * 24 * 3600 * 1000);
    const past30Str = past30.toISOString()
      .split('T')[0];

    const { data: futureRows } = await sb
      .from('schedules')
      .select('id,voyage_no,port_code')
      .eq('vessel_code', vc)
      .gte('eta', past30Str);

    // Group by voyage
    const futureVoys = new Set();
    (futureRows || []).forEach(r => {
      futureVoys.add(r.voyage_no);
    });

    let updatedCount = 0;
    for (const fvoy of futureVoys) {
      const fraw = await kmtcFetch(vc, fvoy);
      if (!fraw.length) continue;
      const frows = normalizePortCalls(
        vc, fvoy, '', fraw);

      // Delete old rows for this voyage
      await sbDelete('schedules', {
        vessel_code: vc,
        voyage_no: fvoy
      });
      // Insert fresh
      await sbPost('schedules', frows);
      updatedCount += frows.length;
    }

    totalUpdated += updatedCount;
    console.log(`${vc}: +${newRows.length} new,` +
      ` ${futureVoys.size} voys refreshed`);

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
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
