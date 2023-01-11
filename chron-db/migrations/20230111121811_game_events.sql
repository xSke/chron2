create table game_events (
    game_id uuid not null,
    timestamp timestamptz not null,
    data jsonb not null,
    search_tsv tsvector default null,
    primary key (game_id, timestamp)
);