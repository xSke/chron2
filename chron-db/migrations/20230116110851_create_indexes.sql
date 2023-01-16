create index versions_sort_idx on versions(kind, valid_from, entity_id);
create index versions_single_idx on versions(kind, entity_id, valid_to);

create index game_events_sort_idx on game_events(timestamp);
create index game_events_single_idx on game_events(game_id, timestamp);
create index game_events_search_idx on game_events using gin(search_tsv);