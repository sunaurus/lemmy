use crate::{
  activities::{
    block::{generate_cc, SiteOrCommunity},
    community::send_activity_in_community,
    generate_activity_id,
    send_lemmy_activity,
    verify_is_public,
    verify_mod_action,
    verify_person_in_community,
  },
  activity_lists::AnnouncableActivities,
  insert_received_activity,
  objects::person::ApubPerson,
  protocol::activities::block::block_user::BlockUser,
};
use activitypub_federation::{
  config::Data,
  kinds::{activity::BlockType, public},
  protocol::verification::verify_domains_match,
  traits::{ActivityHandler, Actor},
};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use lemmy_api_common::{
  context::LemmyContext,
  utils::{remove_user_data, remove_user_data_in_community},
};
use lemmy_db_schema::{
  source::{
    activity::ActivitySendTargets,
    community::{
      CommunityFollower,
      CommunityFollowerForm,
      CommunityPersonBan,
      CommunityPersonBanForm,
    },
    moderator::{ModBan, ModBanForm, ModBanFromCommunity, ModBanFromCommunityForm},
    person::{Person, PersonUpdateForm},
  },
  traits::{Bannable, Crud, Followable},
};
use lemmy_utils::error::LemmyError;
use tracing::log::info;
use url::Url;

impl BlockUser {
  pub(in crate::activities::block) async fn new(
    target: &SiteOrCommunity,
    user: &ApubPerson,
    mod_: &ApubPerson,
    remove_data: Option<bool>,
    reason: Option<String>,
    expires: Option<DateTime<Utc>>,
    context: &Data<LemmyContext>,
  ) -> Result<BlockUser, LemmyError> {
    let audience = if let SiteOrCommunity::Community(c) = target {
      Some(c.id().into())
    } else {
      None
    };
    Ok(BlockUser {
      actor: mod_.id().into(),
      to: vec![public()],
      object: user.id().into(),
      cc: generate_cc(target, &mut context.pool()).await?,
      target: target.id(),
      kind: BlockType::Block,
      remove_data,
      summary: reason,
      id: generate_activity_id(
        BlockType::Block,
        &context.settings().get_protocol_and_hostname(),
      )?,
      audience,
      expires,
    })
  }

  #[tracing::instrument(skip_all)]
  pub async fn send(
    target: &SiteOrCommunity,
    user: &ApubPerson,
    mod_: &ApubPerson,
    remove_data: bool,
    reason: Option<String>,
    expires: Option<DateTime<Utc>>,
    context: &Data<LemmyContext>,
  ) -> Result<(), LemmyError> {
    let block = BlockUser::new(
      target,
      user,
      mod_,
      Some(remove_data),
      reason,
      expires,
      context,
    )
    .await?;

    match target {
      SiteOrCommunity::Site(_) => {
        let inboxes = ActivitySendTargets::to_all_instances();
        send_lemmy_activity(context, block, mod_, inboxes, false).await
      }
      SiteOrCommunity::Community(c) => {
        let activity = AnnouncableActivities::BlockUser(block);
        let inboxes = ActivitySendTargets::to_inbox(user.shared_inbox_or_inbox());
        send_activity_in_community(activity, mod_, c, inboxes, true, context).await
      }
    }
  }
}

#[async_trait::async_trait]
impl ActivityHandler for BlockUser {
  type DataType = LemmyContext;
  type Error = LemmyError;

  fn id(&self) -> &Url {
    &self.id
  }

  fn actor(&self) -> &Url {
    self.actor.inner()
  }

  #[tracing::instrument(skip_all)]
  async fn verify(&self, context: &Data<LemmyContext>) -> Result<(), LemmyError> {
    log_for_adminbot(self.actor.inner().clone(), 1);
    verify_is_public(&self.to, &self.cc)?;
    log_for_adminbot(self.actor.inner().clone(), 2);
    match self.target.dereference(context).await? {
      SiteOrCommunity::Site(site) => {
        log_for_adminbot(self.actor.inner().clone(), 3);
        let domain = self.object.inner().domain().expect("url needs domain");
        log_for_adminbot(self.actor.inner().clone(), 4);
        if context.settings().hostname == domain {
          log_for_adminbot(self.actor.inner().clone(), 5);
          return Err(
            anyhow!("Site bans from remote instance can't affect user's home instance").into(),
          );
        }
        log_for_adminbot(self.actor.inner().clone(), 6);
        // site ban can only target a user who is on the same instance as the actor (admin)
        verify_domains_match(&site.id(), self.actor.inner())?;
        log_for_adminbot(self.actor.inner().clone(), 7);
        verify_domains_match(&site.id(), self.object.inner())?;
        log_for_adminbot(self.actor.inner().clone(), 8);
      }
      SiteOrCommunity::Community(community) => {
        log_for_adminbot(self.actor.inner().clone(), 9);
        verify_person_in_community(&self.actor, &community, context).await?;
        log_for_adminbot(self.actor.inner().clone(), 10);
        verify_mod_action(&self.actor, &community, context).await?;
        log_for_adminbot(self.actor.inner().clone(), 11);
      }
    }
    log_for_adminbot(self.actor.inner().clone(), 12);

    Ok(())
  }

  #[tracing::instrument(skip_all)]
  async fn receive(self, context: &Data<LemmyContext>) -> Result<(), LemmyError> {
    log_for_adminbot(self.actor.inner().clone(), 13);

    insert_received_activity(&self.id, context).await?;
    log_for_adminbot(self.actor.inner().clone(), 14);

    let expires = self.expires.map(Into::into);
    log_for_adminbot(self.actor.inner().clone(), 15);

    let mod_person = self.actor.dereference(context).await?;
    log_for_adminbot(self.actor.inner().clone(), 16);

    let blocked_person = self.object.dereference(context).await?;
    log_for_adminbot(self.actor.inner().clone(), 17);

    let target = self.target.dereference(context).await?;
    log_for_adminbot(self.actor.inner().clone(), 18);
    match target {
      SiteOrCommunity::Site(_site) => {
        log_for_adminbot(self.actor.inner().clone(), 19);
        let blocked_person = Person::update(
          &mut context.pool(),
          blocked_person.id,
          &PersonUpdateForm {
            banned: Some(true),
            ban_expires: Some(expires),
            ..Default::default()
          },
        )
        .await?;
        log_for_adminbot(self.actor.inner().clone(), 20);

        if self.remove_data.unwrap_or(false) {
          log_for_adminbot(self.actor.inner().clone(), 21);
          remove_user_data(blocked_person.id, context).await?;
        }

        log_for_adminbot(self.actor.inner().clone(), 22);

        // write mod log
        let form = ModBanForm {
          mod_person_id: mod_person.id,
          other_person_id: blocked_person.id,
          reason: self.summary,
          banned: Some(true),
          expires,
        };
        log_for_adminbot(self.actor.inner().clone(), 23);

        ModBan::create(&mut context.pool(), &form).await?;
        log_for_adminbot(self.actor.inner().clone(), 24);
      }
      SiteOrCommunity::Community(community) => {
        log_for_adminbot(self.actor.inner().clone(), 25);

        let community_user_ban_form = CommunityPersonBanForm {
          community_id: community.id,
          person_id: blocked_person.id,
          expires: Some(expires),
        };
        CommunityPersonBan::ban(&mut context.pool(), &community_user_ban_form).await?;
        log_for_adminbot(self.actor.inner().clone(), 26);

        // Also unsubscribe them from the community, if they are subscribed
        let community_follower_form = CommunityFollowerForm {
          community_id: community.id,
          person_id: blocked_person.id,
          pending: false,
        };
        log_for_adminbot(self.actor.inner().clone(), 27);

        CommunityFollower::unfollow(&mut context.pool(), &community_follower_form)
          .await
          .ok();

        log_for_adminbot(self.actor.inner().clone(), 28);

        if self.remove_data.unwrap_or(false) {
          remove_user_data_in_community(community.id, blocked_person.id, &mut context.pool())
            .await?;
        }
        log_for_adminbot(self.actor.inner().clone(), 29);

        // write to mod log
        let form = ModBanFromCommunityForm {
          mod_person_id: mod_person.id,
          other_person_id: blocked_person.id,
          community_id: community.id,
          reason: self.summary,
          banned: Some(true),
          expires,
        };
        log_for_adminbot(self.actor.inner().clone(), 30);

        ModBanFromCommunity::create(&mut context.pool(), &form).await?;
      }
    }
    log_for_adminbot(self.actor.inner().clone(), 31);

    Ok(())
  }
}

fn log_for_adminbot(actor: Url, breakpoint: i8) {
  if actor.host_str() == Some("lemm.ee") {
    if actor.path() == "/u/adminbot" {
      info!("adminbot@lemm.ee-debug:{}", breakpoint);
    }
  }
}
