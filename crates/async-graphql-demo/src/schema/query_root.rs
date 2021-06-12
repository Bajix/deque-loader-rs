use crate::data::*;
use async_graphql::{Context, ErrorExtensions, FieldResult, Object};
use channel_loader::{diesel::SimpleDieselError, Loadable};
use db::schema::users;
use diesel::prelude::*;
use diesel_connection::get_connection;
use tokio::task::spawn_blocking;

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
  async fn status(&self) -> i32 {
    200
  }

  async fn users(&self, _ctx: &Context<'_>) -> FieldResult<Vec<User>> {
    let users: Result<Vec<User>, SimpleDieselError> = spawn_blocking(move || {
      let conn = get_connection()?;

      let users = users::table
        .select(users::all_columns)
        .get_results::<User>(&conn)?;

      Ok(users)
    })
    .await
    .unwrap();

    users.map_err(|err| err.extend())
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
