use chrono::NaiveDateTime;
use clokwerk::{Scheduler, TimeUnits};
use diesel::{dsl::now, result::Error, Connection, ExpressionMethods, QueryDsl, Queryable};
// Import week days and WeekDay
use diesel::{sql_query, PgConnection, RunQueryDsl};
use lemmy_db_schema::{
  schema::{comment_aggregates, community_aggregates, post_aggregates},
  utils::{functions::hot_rank, naive_now},
};
use lemmy_utils::error::LemmyError;
use std::{thread, time::Duration};
use tracing::{error, info};

/// Schedules various cleanup tasks for lemmy in a background thread
pub fn setup(db_url: String) -> Result<(), LemmyError> {
  // Setup the connections
  let mut scheduler = Scheduler::new();

  let mut conn_1 = PgConnection::establish(&db_url).expect("could not establish connection");
  let mut conn_2 = PgConnection::establish(&db_url).expect("could not establish connection");
  let mut conn_3 = PgConnection::establish(&db_url).expect("could not establish connection");
  let mut conn_4 = PgConnection::establish(&db_url).expect("could not establish connection");

  active_counts(&mut conn_1);
  update_banned_when_expired(&mut conn_1);
  update_hot_ranks(&mut conn_1, false);
  clear_old_activities(&mut conn_1);

  scheduler.every(1.hour()).run(move || {
    active_counts(&mut conn_2);
    update_banned_when_expired(&mut conn_2);
  });
  // Clear old activities every week
  scheduler.every(TimeUnits::weeks(1)).run(move || {
    clear_old_activities(&mut conn_3);
  });
  scheduler.every(TimeUnits::minutes(15)).run(move || {
    update_hot_ranks(&mut conn_4, true);
  });

  // Manually run the scheduler in an event loop
  loop {
    scheduler.run_pending();
    thread::sleep(Duration::from_millis(1000));
  }
}

#[derive(Queryable, Debug)]
struct HotRanksUpdateResult {
  #[diesel(sql_type = Integer)]
  id: i32,
  #[diesel(sql_type = Timestamp)]
  published: NaiveDateTime,
}

/// Update the hot_rank columns for the aggregates tables
/// Runs in batches until all necessary rows are updated once
fn update_hot_ranks(conn: &mut PgConnection, last_week_only: bool) {
  let update_batch_size = 1000;
  let process_start_time = if last_week_only {
    info!("Updating hot ranks for last week...");
    naive_now() - chrono::Duration::days(14)
  } else {
    info!("Updating hot ranks for all history...");
    NaiveDateTime::from_timestamp_opt(0, 0).unwrap()
  };

  let aggregates_table = post_aggregates::table;
  let published_col = post_aggregates::published;
  let id_col = post_aggregates::id;
  let set = (
    post_aggregates::hot_rank.eq(hot_rank(post_aggregates::score, published_col)),
    post_aggregates::hot_rank_active.eq(hot_rank(
      post_aggregates::score,
      post_aggregates::newest_comment_time_necro,
    )),
    post_aggregates::hot_rank_updated.eq(now),
  );

  let mut batch_start_time = process_start_time;
  loop {
    let transaction = conn.transaction::<Option<NaiveDateTime>, Error, _>(|conn| {
      let batch = aggregates_table
        .select((id_col, published_col))
        .filter(published_col.gt(batch_start_time))
        .order(published_col.asc())
        .limit(update_batch_size)
        .for_update()
        .skip_locked()
        .load::<HotRanksUpdateResult>(conn)?;

      if batch.len() == 0 {
        return Ok(None);
      }
      let last_published = Some(batch.last().unwrap().published);

      diesel::update(aggregates_table)
        .set(set)
        .filter(id_col.eq_any(batch.into_iter().map(|row| row.id)))
        .execute(conn)?;

      return Ok(last_published);
    });

    match transaction {
      Ok(result) => match result {
        Some(batch_last_published) => {
          batch_start_time = batch_last_published;
        }
        None => {
          info!("Finished updating post_aggregates hot ranks");
          break;
        }
      },
      Err(e) => {
        error!("Failed to update post_aggregates hot ranks: {}", e);
        break;
      }
    };
  }

  let mut batch_start_time = process_start_time;

  loop {
    let transaction = conn.transaction::<Option<NaiveDateTime>, Error, _>(|conn| {
      let batch = comment_aggregates::table
        .select((comment_aggregates::id, comment_aggregates::published))
        .filter(comment_aggregates::published.gt(batch_start_time))
        .order(comment_aggregates::published.asc())
        .limit(update_batch_size)
        .for_update()
        .skip_locked()
        .load::<HotRanksUpdateResult>(conn)?;

      if batch.len() == 0 {
        return Ok(None);
      }
      let last_published = Some(batch.last().unwrap().published);

      diesel::update(comment_aggregates::table)
        .set((
          comment_aggregates::hot_rank.eq(hot_rank(
            comment_aggregates::score,
            comment_aggregates::published,
          )),
          comment_aggregates::hot_rank_updated.eq(now),
        ))
        .filter(comment_aggregates::id.eq_any(batch.into_iter().map(|row| row.id)))
        .execute(conn)?;

      return Ok(last_published);
    });

    match transaction {
      Ok(result) => match result {
        Some(batch_last_published) => {
          batch_start_time = batch_last_published;
        }
        None => {
          info!("Finished updating comment_aggregates hot ranks");
          break;
        }
      },
      Err(e) => {
        error!("Failed to update comment_aggregates hot ranks: {}", e);
        break;
      }
    };
  }

  let mut batch_start_time = process_start_time;

  loop {
    let transaction = conn.transaction::<Option<NaiveDateTime>, Error, _>(|conn| {
      let batch = community_aggregates::table
        .select((community_aggregates::id, community_aggregates::published))
        .filter(community_aggregates::published.gt(batch_start_time))
        .order(community_aggregates::published.asc())
        .limit(update_batch_size)
        .for_update()
        .skip_locked()
        .load::<HotRanksUpdateResult>(conn)?;

      if batch.len() == 0 {
        return Ok(None);
      }
      let last_published = Some(batch.last().unwrap().published);

      diesel::update(community_aggregates::table)
        .set((
          community_aggregates::hot_rank.eq(hot_rank(
            community_aggregates::subscribers,
            community_aggregates::published,
          )),
          community_aggregates::hot_rank_updated.eq(now),
        ))
        .filter(community_aggregates::id.eq_any(batch.into_iter().map(|row| row.id)))
        .execute(conn)?;

      return Ok(last_published);
    });

    match transaction {
      Ok(result) => match result {
        Some(batch_last_published) => {
          batch_start_time = batch_last_published;
        }
        None => {
          info!("Finished updating community_aggregates hot ranks");
          break;
        }
      },
      Err(e) => {
        error!("Failed to update community_aggregates hot ranks: {}", e);
        break;
      }
    };
  }

  info!("Finished hot ranks update",);
}

/// Clear old activities (this table gets very large)
fn clear_old_activities(conn: &mut PgConnection) {
  use diesel::dsl::IntervalDsl;
  use lemmy_db_schema::schema::activity::dsl::{activity, published};
  info!("Clearing old activities...");
  match diesel::delete(activity.filter(published.lt(now - 6.months()))).execute(conn) {
    Ok(_) => {
      info!("Done.");
    }
    Err(e) => {
      error!("Failed to clear old activities: {}", e)
    }
  }
}

/// Re-calculate the site and community active counts every 12 hours
fn active_counts(conn: &mut PgConnection) {
  info!("Updating active site and community aggregates ...");

  let intervals = vec![
    ("1 day", "day"),
    ("1 week", "week"),
    ("1 month", "month"),
    ("6 months", "half_year"),
  ];

  for i in &intervals {
    let update_site_stmt = format!(
      "update site_aggregates set users_active_{} = (select * from site_aggregates_activity('{}'))",
      i.1, i.0
    );
    match sql_query(update_site_stmt).execute(conn) {
      Ok(_) => {}
      Err(e) => {
        error!("Failed to update site stats: {}", e)
      }
    }

    let update_community_stmt = format!("update community_aggregates ca set users_active_{} = mv.count_ from community_aggregates_activity('{}') mv where ca.community_id = mv.community_id_", i.1, i.0);
    match sql_query(update_community_stmt).execute(conn) {
      Ok(_) => {}
      Err(e) => {
        error!("Failed to update community stats: {}", e)
      }
    }
  }

  info!("Done.");
}

/// Set banned to false after ban expires
fn update_banned_when_expired(conn: &mut PgConnection) {
  info!("Updating banned column if it expires ...");
  let update_ban_expires_stmt =
    "update person set banned = false where banned = true and ban_expires < now()";
  match sql_query(update_ban_expires_stmt).execute(conn) {
    Ok(_) => {}
    Err(e) => {
      error!("Failed to update expired bans: {}", e)
    }
  }
}
