-- This file should undo anything in `up.sql`
alter table comment_aggregates
    drop column hot_rank_updated;
alter table post_aggregates
    drop column hot_rank_updated;
alter table community_aggregates
    drop column hot_rank_updated;