use crate::data::Bookmark;
use async_graphql::{Context, ErrorExtensions, FieldResult, Object};
use channel_loader::diesel::SimpleDieselError;
use db::schema::{content, users_content};
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

  async fn user_bookmarks(&self, _ctx: &Context<'_>, user_id: i32) -> FieldResult<Vec<Bookmark>> {
    let bookmarks: Result<Vec<Bookmark>, SimpleDieselError> = spawn_blocking(move || {
      let conn = get_connection()?;

      let bookmarks = users_content::table
        .filter(users_content::user_id.eq(user_id))
        .inner_join(content::table)
        .select((users_content::user_id, content::all_columns))
        .get_results::<Bookmark>(&conn)?;

      Ok(bookmarks)
    })
    .await
    .unwrap();

    bookmarks.map_err(|err| err.extend())
  }
}
