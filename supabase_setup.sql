-- รันครั้งเดียวใน Supabase > SQL Editor
create table if not exists public.bot_store (
  key        text primary key,
  value      jsonb not null,
  updated_at timestamptz not null default now()
);

-- อัปเดตเวลาแก้ไขล่าสุดอัตโนมัติ
create or replace function public.bot_store_touch()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists bot_store_touch on public.bot_store;
create trigger bot_store_touch before update on public.bot_store
for each row execute function public.bot_store_touch();

-- ปิดไม่ให้คนนอกอ่าน/เขียน (ระบบใช้ service_role key ซึ่งผ่านได้)
alter table public.bot_store enable row level security;
