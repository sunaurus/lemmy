-- Your SQL goes here

alter table comment_aggregates
    add column hot_rank_updated timestamp default now();
alter table post_aggregates
    add column hot_rank_updated timestamp default now();
alter table community_aggregates
    add column hot_rank_updated timestamp default now();