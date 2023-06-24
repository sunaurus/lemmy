use chrono::NaiveDateTime;
use clokwerk::{Scheduler, TimeUnits};
use diesel::{
  dsl::now,
  result::Error,
  sql_types::{Integer, Timestamp},
  Connection,
  ExpressionMethods,
  QueryDsl,
  QueryableByName,
};
// Import week days and WeekDay
use diesel::{sql_query, PgConnection, RunQueryDsl};
use lemmy_db_schema::utils::naive_now;
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

#[derive(QueryableByName)]
struct HotRanksUpdateResult {
  #[diesel(sql_type = Timestamp)]
  published: NaiveDateTime,
}

/// Update the hot_rank columns for the aggregates tables
/// Runs in batches until all necessary rows are updated once
fn update_hot_ranks(conn: &mut PgConnection, last_week_only: bool) {
  let process_start_time = if last_week_only {
    info!("Updating hot ranks for last week...");
    naive_now() - chrono::Duration::days(7)
  } else {
    info!("Updating hot ranks for all history...");
    NaiveDateTime::from_timestamp_opt(0, 0).unwrap()
  };

  process_hot_ranks_in_batches(
    conn,
    "post_aggregates",
    "SET hot_rank = hot_rank(a.score, a.published), 
         hot_rank_active = hot_rank(a.score, a.newest_comment_time_necro),
             hot_rank_updated = now()",
    process_start_time,
  );

  process_hot_ranks_in_batches(
    conn,
    "comment_aggregates",
    "SET hot_rank = hot_rank(a.score, a.published), hot_rank_updated = now()",
    process_start_time,
  );

  process_hot_ranks_in_batches(
    conn,
    "community_aggregates",
    "SET hot_rank = hot_rank(a.subscribers, a.published)",
    process_start_time,
  );

  info!("Finished hot ranks update!");
}

/// Runs the hot rank update query in batches until all ids have been processed.
/// In `set_clause`, "a" will refer to the current aggregates table.
/// Locked rows are skipped in order to prevent deadlocks (they will likely get updated on the next
/// run)
fn process_hot_ranks_in_batches(
  conn: &mut PgConnection,
  table_name: &str,
  set_clause: &str,
  process_start_time: NaiveDateTime,
) {
  let update_batch_size = 1000; // Bigger batches than this tend to cause seq scans
  let mut previous_batch_last_published = process_start_time;
  loop {
    // Raw `sql_query` is used as a performance optimization - Diesel does not support doing this
    // in a single query (neither as a CTE, nor using a subquery)
    let result = sql_query(format!(
      r#"WITH batch AS (SELECT a.id
               FROM {aggregates_table} a
               WHERE a.published > $1
               ORDER BY a.published
               LIMIT $2
               FOR UPDATE SKIP LOCKED)
         UPDATE {aggregates_table} a {set_clause}
             FROM batch WHERE a.id = batch.id RETURNING a.published;
    "#,
      aggregates_table = table_name,
      set_clause = set_clause
    ))
    .bind::<Timestamp, _>(previous_batch_last_published)
    .bind::<Integer, _>(update_batch_size)
    .get_results::<HotRanksUpdateResult>(conn);

    match result {
      Ok(updated_rows) => match updated_rows.last() {
        Some(row) => {
          previous_batch_last_published = row.published;
        }
        None => {
          info!("Finished processing {} hot ranks!", table_name);
          break;
        }
      },
      Err(e) => {
        match e {
          Error::NotFound => {
            info!("Finished processing {} hot ranks!", table_name)
          }
          _ => {
            error!("Failed to update {} hot_ranks: {}", table_name, e);
          }
        }
        break;
      }
    }
  }
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
