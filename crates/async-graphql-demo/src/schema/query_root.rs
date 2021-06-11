use crate::data::*;
use async_graphql::{Context, ErrorExtensions, FieldResult, Object};
use channel_loader::Loadable;

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
  async fn status(&self) -> i32 {
    200
  }

  async fn user_bookmarks(
    &self,
    _ctx: &Context<'_>,
    user_id: UserId,
  ) -> FieldResult<Vec<Bookmark>> {
    let bookmarks = Bookmark::load_by(user_id)
      .await
      .unwrap()
      .map_err(|err| err.extend())?
      .unwrap_or_else(|| vec![]);

    Ok(bookmarks)
  }
}
