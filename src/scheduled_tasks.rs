use clokwerk::{Scheduler, TimeUnits};
use diesel::{dsl::now, Connection, ExpressionMethods, QueryDsl};
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

/// Update the hot_rank columns for the aggregates tables
/// Runs in batches of 100 until all necessary rows are updated once
fn update_hot_ranks(conn: &mut PgConnection, last_week_only: bool) {
  let update_start_time = naive_now();
  let update_batch_size = 5000;
  let last_week = now - diesel::dsl::IntervalDsl::weeks(1);

  if last_week_only {
    info!("Updating hot ranks for last week...");
  } else {
    info!("Updating hot ranks for all history...");
  }

  loop {
    let mut post_select = post_aggregates::table
      .select(post_aggregates::post_id)
      .filter(post_aggregates::hot_rank_updated.lt(update_start_time))
      .order(post_aggregates::post_id.asc())
      .limit(update_batch_size)
      .into_boxed();

    if last_week_only {
      post_select = post_select.filter(post_aggregates::published.gt(last_week));
    }

    match diesel::update(post_aggregates::table)
      .set((
        post_aggregates::hot_rank.eq(hot_rank(post_aggregates::score, post_aggregates::published)),
        post_aggregates::hot_rank_active.eq(hot_rank(
          post_aggregates::score,
          post_aggregates::newest_comment_time_necro,
        )),
        post_aggregates::hot_rank_updated.eq(now),
      ))
      .filter(post_aggregates::post_id.eq_any(post_select))
      .execute(conn)
    {
      Ok(updated_rows) => {
        if updated_rows == 0 {
          break;
        }
      }
      Err(e) => {
        error!("Failed to update post_aggregates hot_ranks: {}", e);
        break;
      }
    }
  }

  loop {
    let mut comment_select = comment_aggregates::table
      .select(comment_aggregates::comment_id)
      .filter(comment_aggregates::hot_rank_updated.lt(update_start_time))
      .order(comment_aggregates::comment_id.asc())
      .limit(update_batch_size)
      .into_boxed();

    if last_week_only {
      comment_select = comment_select.filter(comment_aggregates::published.gt(last_week));
    }

    match diesel::update(comment_aggregates::table)
      .set((
        comment_aggregates::hot_rank.eq(hot_rank(
          comment_aggregates::score,
          comment_aggregates::published,
        )),
        comment_aggregates::hot_rank_updated.eq(now),
      ))
      .filter(comment_aggregates::comment_id.eq_any(comment_select))
      .execute(conn)
    {
      Ok(updated_rows) => {
        if updated_rows == 0 {
          break;
        }
      }
      Err(e) => {
        error!("Failed to update comment_aggregates hot_ranks: {}", e);
        break;
      }
    }
  }
  loop {
    let mut community_select = community_aggregates::table
      .select(community_aggregates::community_id)
      .filter(community_aggregates::hot_rank_updated.lt(update_start_time))
      .order(community_aggregates::community_id.asc())
      .limit(update_batch_size)
      .into_boxed();

    if last_week_only {
      community_select = community_select.filter(community_aggregates::published.gt(last_week));
    }

    match diesel::update(community_aggregates::table)
      .set((
        community_aggregates::hot_rank.eq(hot_rank(
          community_aggregates::subscribers,
          community_aggregates::published,
        )),
        community_aggregates::hot_rank_updated.eq(now),
      ))
      .filter(community_aggregates::community_id.eq_any(community_select))
      .execute(conn)
    {
      Ok(updated_rows) => {
        if updated_rows == 0 {
          break;
        }
      }
      Err(e) => {
        error!("Failed to update community_aggregates hot_ranks: {}", e);
        break;
      }
    }
  }

  info!(
    "Finished hot ranks update which started {}",
    update_start_time
  );
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
