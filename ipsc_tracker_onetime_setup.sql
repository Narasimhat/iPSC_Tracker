-- ipsc_tracker_onetime_setup.sql
-- =========================================
-- One-shot bootstrap for IPSC Tracker in Snowflake
-- Requires SnowSQL >= 1.2 and CSVs (logs.csv, users.csv, reference_lists.csv)
-- Run:
-- snowsql -f ipsc_tracker_onetime_setup.sql -o variable_substitution=true \
--   -D LOGS=/path/to/logs_for_snowflake.csv \
--   -D USERS=/path/to/users_for_snowflake.csv \
--   -D REFS=/path/to/reference_lists_for_snowflake.csv
-- =========================================

!set variable_substitution=true;
!set LOGS       = &{LOGS:-logs_for_snowflake.csv};
!set USERS      = &{USERS:-users_for_snowflake.csv};
!set REFS       = &{REFS:-reference_lists_for_snowflake.csv};

create database if not exists IPSC_TRACKER_DB;
create schema    if not exists IPSC_TRACKER_DB.PUBLIC;

create warehouse if not exists IPSC_TRACKER_WH
  warehouse_size = 'XSMALL'
  auto_suspend   = 60
  auto_resume    = true;

use warehouse IPSC_TRACKER_WH;
use database  IPSC_TRACKER_DB;
use schema    PUBLIC;

create table if not exists users (
  id integer autoincrement,
  name varchar not null,
  initials varchar,
  email varchar,
  color_hex varchar,
  is_active boolean default true,
  primary key (id)
);

create table if not exists culture_logs (
  id integer autoincrement,
  created_at timestamp_ntz default current_timestamp,
  updated_at timestamp_ntz,
  entry_date date not null,
  cell_line varchar,
  thaw_id varchar,
  cell_type varchar,
  medium varchar,
  vessel varchar,
  cryovial_position varchar,
  storage_location varchar,
  plate_location varchar,
  volume_ml float,
  event_type varchar,
  passage integer,
  confluency varchar,
  action varchar,
  next_action_date date,
  assigned_to varchar,
  notes varchar,
  created_by varchar,
  marked_done boolean default false,
  done_at timestamp_ntz,
  primary key (id)
);

create table if not exists weekend_schedule (
  id integer autoincrement,
  start_date date not null,
  end_date date not null,
  assignee varchar not null,
  created_at timestamp_ntz default current_timestamp,
  notes varchar,
  primary key (id)
);

create table if not exists workflow_templates (
  id integer autoincrement,
  name varchar not null,
  steps variant,
  primary key (id)
);

create table if not exists reference_lists (
  id integer autoincrement,
  list_type varchar not null,
  value varchar not null,
  metadata variant,
  primary key (id)
);

alter table if exists culture_logs add column if not exists experiment_type string;
alter table if exists culture_logs add column if not exists experiment_stage string;
alter table if exists culture_logs add column if not exists experimental_conditions string;
alter table if exists culture_logs add column if not exists protocol_reference string;
alter table if exists culture_logs add column if not exists outcome_status string;
alter table if exists culture_logs add column if not exists success_metrics string;
alter table if exists culture_logs add column if not exists linked_thaw_id string;
alter table if exists culture_logs add column if not exists operator string;

create or replace file format CSV_STD
  type = csv
  field_delimiter = ','
  skip_header = 1
  empty_field_as_null = true
  null_if = ('', 'NULL');

create or replace stage MIGR_STAGE;

-- After this file runs up to here, in SnowSQL run:
--  !put file://&{LOGS}  @MIGR_STAGE auto_compress=false;
--  !put file://&{USERS} @MIGR_STAGE auto_compress=false;
--  !put file://&{REFS}  @MIGR_STAGE auto_compress=false;

copy into users (name, initials, email, color_hex, is_active)
from (
  select
    t.$1::string as name,
    t.$2::string as initials,
    t.$3::string as email,
    t.$4::string as color_hex,
    t.$5::boolean as is_active
  from @MIGR_STAGE/&{USERS} (file_format=>CSV_STD) t
)
on_error = 'continue';

copy into reference_lists (list_type, value, metadata)
from (
  select
    t.$1::string as list_type,
    t.$2::string as value,
    try_parse_json(t.$3::string) as metadata
  from @MIGR_STAGE/&{REFS} (file_format=>CSV_STD) t
)
on_error = 'continue';

copy into culture_logs (
  entry_date,
  cell_line,
  thaw_id,
  cell_type,
  medium,
  vessel,
  cryovial_position,
  storage_location,
  volume_ml,
  event_type,
  passage,
  assigned_to,
  notes,
  created_by,
  created_at,
  next_action_date
)
from (
  select
    to_date(t.$1)               as entry_date,
    nullif(t.$2,'')::string     as cell_line,
    nullif(t.$3,'')::string     as thaw_id,
    nullif(t.$4,'')::string     as cell_type,
    nullif(t.$5,'')::string     as medium,
    nullif(t.$6,'')::string     as vessel,
    nullif(t.$7,'')::string     as cryovial_position,
    nullif(t.$8,'')::string     as storage_location,
    try_to_double(t.$9)         as volume_ml,
    nullif(t.$10,'')::string    as event_type,
    try_to_number(t.$11)        as passage,
    nullif(t.$12,'')::string    as assigned_to,
    nullif(t.$13,'')::string    as notes,
    nullif(t.$14,'')::string    as created_by,
    try_to_timestamp_ntz(t.$15) as created_at,
    iff(nullif(t.$16,'') is null, null, to_date(t.$16)) as next_action_date
  from @MIGR_STAGE/&{LOGS} (file_format=>CSV_STD) t
)
on_error = 'continue';

create or replace view logs as
select
  id,
  entry_date as "date",
  cell_line,
  event_type,
  passage,
  vessel,
  storage_location as "location",
  medium,
  cell_type,
  notes,
  operator,
  thaw_id,
  cryovial_position as cryo_vial_position,
  null::varchar as image_path,
  assigned_to,
  next_action_date,
  created_by,
  coalesce(created_at, current_timestamp()) as created_at,
  volume_ml as volume,
  experiment_type,
  experiment_stage,
  experimental_conditions,
  protocol_reference,
  outcome_status,
  success_metrics,
  linked_thaw_id
from culture_logs;

create or replace function derive_culture_stage(event_type string, explicit_stage string)
returns string
language sql
as
$$
  coalesce(
    nullif(explicit_stage,''),
    case upper(event_type)
      when 'THAWING'               then 'Recovery'
      when 'MEDIA CHANGE'          then 'Maintenance'
      when 'OBSERVATION'           then 'Maintenance'
      when 'SPLIT'                 then 'Expansion'
      when 'PASSAGE'               then 'Expansion'
      when 'DIFFERENTIATION START' then 'Differentiation'
      when 'DIFFERENTIATION'       then 'Differentiation'
      when 'CRYOPRESERVATION'      then 'Banking'
      when 'BANKING'               then 'Banking'
      else 'General Maintenance'
    end
  )
$$;

create or replace procedure next_thaw_id(entry_d date, operator string, cell_type string)
returns string
language javascript
as
$$
function initials(op) {
  if (!op) return '';
  const parts = op.trim().split(/\s+/);
  if (parts.length >= 2) return (parts[0][0] + parts[parts.length-1][0]).toUpperCase();
  return parts[0].slice(0,2).toUpperCase();
}
function ct_code(ct) {
  if (!ct) return '';
  const s = ct.trim().toUpperCase();
  if (s === 'IPSC') return 'iPSC';
  if (['FIBROBLAST','FIBRO'].includes(s)) return 'FIBRO';
  if (['NPC','NEURAL'].includes(s)) return 'NPC';
  if (['CARDIOMYOCYTE','CARDIO','CM'].includes(s)) return 'CARDIO';
  if (['HEPATOCYTE','HEPATO','HEP'].includes(s)) return 'HEPATO';
  if (['ENDOTHELIAL','ENDO','EC'].includes(s)) return 'ENDO';
  return s.slice(0,5);
}
const day = entry_d.toISOString().slice(0,10).replace(/-/g,'');
const op = initials(operator || '');
const ct = ct_code(cell_type || '');
let base = `TH-${day}`;
if (op) base += `-${op}`;
if (ct) base += `-${ct}`;
const like = base + '-%';

const stmt = snowflake.createStatement({
  sqlText: `select count(*) from culture_logs where thaw_id like ? and entry_date = ?`,
  binds: [like, entry_d]
});
const rs = stmt.execute();
rs.next();
const count = Number(rs.getColumnValue(1)) || 0;
const nextNum = String(count + 1).padStart(3,'0');
return `${base}-${nextNum}`;
$$;

-- Optional analytics queries left out for brevity
