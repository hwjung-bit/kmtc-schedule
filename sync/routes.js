// =============================================================================
// Service route (항로) enrichment
//
// The KMTC gateway that feeds `schedules` carries no service-lane code — its
// payload stops at port/terminal/times. ekmtc.com's public leg search does
// carry one: every leg it returns is tagged with rteCd/rteCdNm.
//
// So we walk our own port calls, turn each voyage into consecutive port pairs,
// ask the leg search about each distinct pair once per month, and copy the
// route onto the port call the leg departs from. A voyage that switches
// service mid-rotation therefore gets a different route on each port call,
// which is the point — route is stored per port call, not per ship.
//
// Results live in their own table (`voyage_routes`) because `schedules` rows
// are deleted and re-inserted on every refresh; a column there would be wiped
// twice a day.
// =============================================================================

const LEG_API =
  'https://api.ekmtc.com/schedule/schedule/leg/search-schedule';
const PLACES_API =
  'https://api.ekmtc.com/common/commons/places';
const SCHEDULE_FORM_API =
  'https://api.ekmtc.com/schedule/schedule/leg/search-scheduleform';

// ekmtc.com is a public web front end, not a metered gateway. Keep a polite
// fixed spacing rather than bursting.
const EKMTC_INTERVAL_MS = 600;
const EKMTC_RETRIES = 2;
// Backstop so a data anomaly can never turn into thousands of requests.
const MAX_LEG_CALLS = 900;

const HEADERS = {
  'Accept': 'application/json, text/plain, */*',
  'Referer': 'https://www.ekmtc.com/',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36'
};

let legCalls = 0;
let legFailures = 0;
let lastEkmtcAt = 0;

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function ekmtcGet(url) {
  for (let attempt = 0; attempt <= EKMTC_RETRIES; attempt++) {
    const wait = lastEkmtcAt + EKMTC_INTERVAL_MS - Date.now();
    if (wait > 0) await sleep(wait);
    lastEkmtcAt = Date.now();
    try {
      const resp = await fetch(url, { headers: HEADERS });
      if (!resp.ok) {
        await sleep(1500 * (attempt + 1));
        continue;
      }
      return await resp.json();
    } catch (e) {
      await sleep(1500 * (attempt + 1));
    }
  }
  return null;
}

// ── Port → country ──────────────────────────────────────────────────────────
// The leg search wants a country code alongside every port code. The bare
// /places call answers with one country's worth of ports regardless of the
// keyword, so walk the country list the schedule form hands out instead.

async function loadPortCountries() {
  const form = await ekmtcGet(SCHEDULE_FORM_API);
  const countries = ((form && form.startCtrCdList) || [])
    .map(c => c.ctrCd).filter(Boolean);
  const map = {};
  for (const ctr of countries) {
    const body = await ekmtcGet(PLACES_API + '/' + ctr);
    if (!Array.isArray(body)) continue;
    body.forEach(p => {
      if (p.plcCd && p.ctrCd && !map[p.plcCd]) {
        map[p.plcCd] = p.ctrCd;
      }
    });
  }
  console.log(`port→country entries: ${Object.keys(map).length} ` +
    `across ${countries.length} countries`);
  return map;
}

// ── Leg search ──────────────────────────────────────────────────────────────

function legParams(polCtr, polCd, podCtr, podCd, year, month) {
  return new URLSearchParams({
    startCtrCd: polCtr, startPlcCd: polCd,
    destCtrCd: podCtr, destPlcCd: podCd,
    searchYear: year, searchMonth: month,
    bound: 'O', eiCatCd: 'O',
    // Direct sailings only — a transhipment leg belongs to a different
    // vessel than the one whose port call we are labelling.
    filterDirect: 'Y', filterTs: 'N',
    filterTranMin: '0', filterTranMax: '0',
    filterYn: 'N', searchYN: 'Y',
    calendarOrList: 'L', main: 'N', legIdx: '0',
    vslType01: '01', vslType03: '03',
    cpYn: 'N', promotionChk: 'N',
    startPlcName: '', destPlcName: '',
    polTrmlStr: '', podTrmlStr: '', rteCd: '',
    filterPolCd: '', filterPodCd: '',
    pointChangeYN: '', pointLength: '',
    hidstartPlcCd: '', hiddestPlcCd: '',
    unno: '', commodityCd: '', vslCd: '', voyNo: ''
  }).toString();
}

// Voyage numbers come back zero-padded from some carriers ("02630W") and
// bare from others ("2630W"). Compare on a common form.
function normVoy(v) {
  return String(v || '').trim().toUpperCase().replace(/^0+/, '');
}

// The gateway reports the terminal a call actually used where ekmtc.com
// reports the parent port, so the two disagree on Busan and Port Klang.
// Same list the web app uses to consolidate ports.
const PORT_ALIAS = {
  PNC: 'PUS', HBGT: 'PUS', HBCT: 'PUS', DGT: 'PUS', BPT: 'PUS',
  BPTS: 'PUS', BNCT: 'PUS', HJNC: 'PUS', HPNT: 'PUS', UNCT: 'PUS',
  PKW: 'PKG', MIP: 'MNL'
};

function normPort(p) {
  const c = String(p || '').trim().toUpperCase();
  return PORT_ALIAS[c] || c;
}

async function fetchLegRoutes(polCtr, polCd, podCtr, podCd,
  year, month, index) {
  if (legCalls >= MAX_LEG_CALLS) return;
  legCalls++;
  const body = await ekmtcGet(
    LEG_API + '?' + legParams(polCtr, polCd, podCtr, podCd, year, month));
  const list = (body && body.listSchedule) || [];
  if (!body) {
    legFailures++;
    return;
  }
  list.forEach(leg => {
    if (!leg.rteCd || !leg.vslCd || !leg.voyNo) return;
    const key = `${leg.vslCd}|${normVoy(leg.voyNo)}` +
      `|${normPort(leg.pol)}|${normPort(leg.pod)}`;
    if (!index.has(key)) {
      index.set(key, {
        route_cd: String(leg.rteCd).trim(),
        route_nm: String(leg.rteCdNm || '').trim()
      });
    }
  });
}

// ── Main ────────────────────────────────────────────────────────────────────

/**
 * Reads the port calls we already hold, resolves a service route for each,
 * and upserts them into `voyage_routes`.
 *
 * @param sb        Supabase client
 * @param opts.days how far ahead/back to cover (default: -45..+150 days)
 * @param opts.dryRun  log what would be written instead of writing
 */
async function syncRoutes(sb, opts) {
  opts = opts || {};
  const backDays = opts.backDays || 45;
  const aheadDays = opts.aheadDays || 150;
  console.log(`=== ROUTE SYNC START (-${backDays}d..+${aheadDays}d) ===`);

  const from = new Date(Date.now() - backDays * 86400000)
    .toISOString().split('T')[0];
  const to = new Date(Date.now() + aheadDays * 86400000)
    .toISOString().split('T')[0];

  // ── 1. Our own port calls in the window ──
  // A voyage clipped by the window edge loses the neighbour that would form
  // its leg, so collect the voyages the window touches and then read those
  // voyages whole.
  const PAGE = 1000;

  async function page(build) {
    const rows = [];
    for (let offset = 0; ; offset += PAGE) {
      const { data, error } = await build()
        .order('vessel_code', { ascending: true })
        .order('voyage_no', { ascending: true })
        .order('eta', { ascending: true })
        .range(offset, offset + PAGE - 1);
      if (error) {
        console.error('Supabase read error:', error.message);
        return null;
      }
      if (!data || !data.length) break;
      rows.push(...data);
      if (data.length < PAGE) break;
    }
    return rows;
  }

  const seed = await page(() => sb
    .from('schedules')
    .select('vessel_code,voyage_no')
    .gte('eta', from).lte('eta', to));
  if (!seed) return;

  const wantVoy = new Set(
    seed.map(r => `${r.vessel_code}|${r.voyage_no}`));
  const wantNos = [...new Set(seed.map(r => r.voyage_no))];
  console.log(`voyages touching window: ${wantVoy.size}`);
  if (!wantVoy.size) return;

  // Filtering on voyage_no alone over-fetches a little (two ships can share
  // a number); the set above trims it back.
  const raw = await page(() => sb
    .from('schedules')
    .select('vessel_code,voyage_no,port_code,eta,etd')
    .in('voyage_no', wantNos));
  if (!raw) return;
  const calls = raw.filter(
    r => wantVoy.has(`${r.vessel_code}|${r.voyage_no}`));
  console.log(`port calls (whole voyages): ${calls.length}`);
  if (!calls.length) return;

  // ── 2. Consecutive port pairs per voyage ──
  const voyages = new Map();
  calls.forEach(c => {
    const k = `${c.vessel_code}|${c.voyage_no}`;
    if (!voyages.has(k)) voyages.set(k, []);
    voyages.get(k).push(c);
  });

  const portCtr = await loadPortCountries();
  const queries = new Map();   // dedup key → query args
  const legsOf = new Map();    // voyage key → [{from, to, year, month}]

  voyages.forEach((rows, vkey) => {
    rows.sort((a, b) => new Date(a.eta) - new Date(b.eta));
    const legs = [];
    for (let i = 0; i < rows.length - 1; i++) {
      const a = rows[i], b = rows[i + 1];
      const pol = normPort(a.port_code);
      const pod = normPort(b.port_code);
      // Two calls at the same parent port (a Busan terminal shift) are not
      // a leg the schedule search knows about.
      if (pol === pod) continue;
      const polCtr = portCtr[pol];
      const podCtr = portCtr[pod];
      if (!polCtr || !podCtr) continue;
      // The leg search buckets by the month the leg sails.
      const d = new Date(a.etd || a.eta);
      if (isNaN(d)) continue;
      const year = String(d.getUTCFullYear());
      const month = String(d.getUTCMonth() + 1).padStart(2, '0');
      const q = [polCtr, pol, podCtr, pod, year, month];
      queries.set(q.join('|'), q);
      legs.push({ port: a.port_code, from: pol, to: pod });
    }
    legsOf.set(vkey, legs);
  });

  console.log(`voyages: ${voyages.size}, distinct leg queries: ` +
    `${queries.size}`);

  // ── 3. Ask ekmtc.com about each distinct pair-month ──
  const index = new Map();
  let done = 0;
  for (const q of queries.values()) {
    await fetchLegRoutes(q[0], q[1], q[2], q[3], q[4], q[5], index);
    done++;
    if (done % 25 === 0) {
      console.log(`  leg queries ${done}/${queries.size}, ` +
        `${index.size} legs indexed`);
    }
    if (legCalls >= MAX_LEG_CALLS) {
      console.error(`leg call cap (${MAX_LEG_CALLS}) reached — ` +
        `${queries.size - done} queries skipped this run`);
      break;
    }
  }
  console.log(`indexed legs: ${index.size} ` +
    `(${legCalls} calls, ${legFailures} failed)`);

  // ── 4. Route per port call ──
  //
  // Three passes, widening only when the narrower one found nothing:
  //   a. the leg this port call departs on
  //   b. the rest of the voyage, then the opposite bound of the same voyage
  //      number (2607E and 2607W are the two halves of one loop)
  //   c. the vessel's nearest voyage in time
  // A backhaul voyage often holds a single port call in our data, so it has
  // no leg of its own to look up — (b) is what labels it.
  const stamp = new Date().toISOString();
  const resolved = new Map();   // voyage key → [hit|null] aligned with rows
  const byStem = new Map();     // 'vessel|2607' → hit
  const byVessel = new Map();   // vessel → [{t, hit}]

  function voyStem(v) {
    return String(v || '').toUpperCase().replace(/[A-Z]+$/, '');
  }

  voyages.forEach((rows, vkey) => {
    const [vessel, voyage] = vkey.split('|');
    const nv = normVoy(voyage);
    const legs = legsOf.get(vkey) || [];

    const perPort = new Map();
    legs.forEach(leg => {
      const hit = index.get(`${vessel}|${nv}|${leg.from}|${leg.to}`);
      if (hit) perPort.set(leg.port, hit);
    });

    // (a) + spread across the voyage: the last port has no outbound leg, and
    // a leg the search did not cover leaves a hole.
    const filled = rows.map(r => perPort.get(r.port_code) || null);
    for (let i = 1; i < filled.length; i++) {
      if (!filled[i]) filled[i] = filled[i - 1];
    }
    for (let i = filled.length - 2; i >= 0; i--) {
      if (!filled[i]) filled[i] = filled[i + 1];
    }
    resolved.set(vkey, filled);

    const hit = filled.find(Boolean);
    if (hit) {
      byStem.set(`${vessel}|${voyStem(voyage)}`, hit);
      if (!byVessel.has(vessel)) byVessel.set(vessel, []);
      byVessel.get(vessel).push({
        t: new Date(rows[0].eta).getTime(), hit
      });
    }
  });

  byVessel.forEach(list => list.sort((x, y) => x.t - y.t));

  const out = [];
  let fromLeg = 0, fromStem = 0, fromNeighbour = 0;

  voyages.forEach((rows, vkey) => {
    const [vessel, voyage] = vkey.split('|');
    const filled = resolved.get(vkey) || [];

    let fallback = null, source = '';
    if (!filled.some(Boolean)) {
      // (b) opposite bound of the same voyage number
      fallback = byStem.get(`${vessel}|${voyStem(voyage)}`) || null;
      source = 'stem';
      if (!fallback) {
        // (c) whichever voyage of this vessel sails closest in time
        const list = byVessel.get(vessel) || [];
        const t = new Date(rows[0].eta).getTime();
        let best = null, bestGap = Infinity;
        list.forEach(e => {
          const gap = Math.abs(e.t - t);
          if (gap < bestGap) { bestGap = gap; best = e.hit; }
        });
        fallback = best;
        source = 'neighbour';
      }
      if (!fallback) return;
    }

    rows.forEach((r, i) => {
      const hit = filled[i] || fallback;
      if (!hit) return;
      if (filled[i]) fromLeg++;
      else if (source === 'stem') fromStem++;
      else fromNeighbour++;
      out.push({
        vessel_code: vessel,
        voyage_no: voyage,
        port_code: r.port_code,
        route_cd: hit.route_cd,
        route_nm: hit.route_nm,
        updated_at: stamp
      });
    });
  });

  console.log(`route sources — leg: ${fromLeg}, same voyage no: ` +
    `${fromStem}, nearest voyage: ${fromNeighbour}`);

  console.log(`resolved route for ${out.length} / ${calls.length} ` +
    `port calls`);

  if (opts.dryRun) {
    out.slice(0, 20).forEach(r => console.log('  ', JSON.stringify(r)));
    return out;
  }

  // ── 5. Write ──
  for (let i = 0; i < out.length; i += 500) {
    const { error } = await sb
      .from('voyage_routes')
      .upsert(out.slice(i, i + 500),
        { onConflict: 'vessel_code,voyage_no,port_code' });
    if (error) console.error('voyage_routes upsert error:', error.message);
  }

  // ── 6. Keep the ship's standing route honest ──
  // `ships.route_key` drives the "투입 항로" badge and was hand-maintained, so
  // it drifted. Set it to the route the vessel actually runs most often.
  const tally = new Map();
  out.forEach(r => {
    if (!tally.has(r.vessel_code)) tally.set(r.vessel_code, new Map());
    const t = tally.get(r.vessel_code);
    t.set(r.route_cd, (t.get(r.route_cd) || 0) + 1);
  });
  for (const [vessel, t] of tally) {
    const top = [...t.entries()].sort((a, b) => b[1] - a[1])[0];
    if (!top) continue;
    const { error } = await sb
      .from('ships').update({ route_key: top[0] }).eq('code', vessel);
    if (error) {
      console.error(`ships.route_key ${vessel}:`, error.message);
    }
  }
  console.log(`ships.route_key refreshed for ${tally.size} vessels`);

  console.log('=== ROUTE SYNC DONE ===');
  return out;
}

module.exports = { syncRoutes };
