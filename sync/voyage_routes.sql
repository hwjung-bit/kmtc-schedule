-- Service route (항로) per port call.
--
-- Kept out of `schedules` on purpose: the sync deletes and re-inserts
-- schedule rows on every refresh, so a column there would be wiped twice a
-- day. This table is keyed the same way a port call is, and survives.
--
-- Run once in the Supabase SQL editor.

create table if not exists public.voyage_routes (
  vessel_code text        not null,
  voyage_no   text        not null,
  port_code   text        not null,
  route_cd    text,
  route_nm    text,
  updated_at  timestamptz not null default now(),
  primary key (vessel_code, voyage_no, port_code)
);

alter table public.voyage_routes enable row level security;

-- The web app reads with the public anon key; the sync writes with the
-- service role, which bypasses RLS.
drop policy if exists "voyage_routes read" on public.voyage_routes;
create policy "voyage_routes read"
  on public.voyage_routes
  for select
  to anon, authenticated
  using (true);
