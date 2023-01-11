create extension "pgcrypto";

create table observations (
    kind smallint not null,
    entity_id uuid not null,
    timestamp timestamptz not null,
    request_time float not null,
    hash uuid not null
);
select create_hypertable('observations', 'timestamp', chunk_time_interval => interval '1 day');
create index observations_idx on observations(kind, entity_id, timestamp desc);

-- used for version "locking", and good for reads too
create table latest_versions (
    kind smallint not null,
    entity_id uuid not null,
    seq int not null,
    hash uuid not null,
    valid_from timestamptz not null,
    primary key (kind, entity_id)
);

create table versions (
    kind smallint not null,
    seq int not null,
    entity_id uuid not null,
    hash uuid not null,
    valid_from timestamptz not null,
    valid_to timestamptz default null,
    last_seen timestamptz default null,
    primary key (kind, entity_id, seq)
);

create table events (
    timestamp timestamptz not null,
    channel text not null,
    event text not null,
    raw text not null,
    payload jsonb
);
create index events_idx on events(channel, timestamp);

create table objects (
    hash uuid primary key,
    data jsonb not null
);