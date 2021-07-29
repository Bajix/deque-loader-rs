use std::sync::Arc;

use crate::data::*;
use async_graphql::{Context, ErrorExtensions, FieldResult, Object};
use deque_loader::{diesel::SimpleDieselError, Loadable};

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
  async fn status(&self) -> i32 {
    200
  }

  async fn users(&self, _ctx: &Context<'_>) -> FieldResult<Arc<Vec<User>>> {
    let users = User::load_by(())
      .await
      .map_err(|err: SimpleDieselError| err.extend())?
      .unwrap_or_else(|| Arc::new(vec![]));

    Ok(users)
  }

  async fn user_bookmarks(
    &self,
    _ctx: &Context<'_>,
    user_id: UserId,
  ) -> FieldResult<Arc<Vec<Bookmark>>> {
    let bookmarks = Bookmark::load_by(user_id)
      .await
      .map_err(|err| err.extend())?
      .unwrap_or_else(|| Arc::new(vec![]));

    Ok(bookmarks)
  }
}
