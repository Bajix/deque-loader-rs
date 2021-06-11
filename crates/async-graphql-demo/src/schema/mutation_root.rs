use crate::data::*;
use async_graphql::{Context, ErrorExtensions, FieldResult, Object};
use channel_loader::diesel::SimpleDieselError;
use db::schema::{content, users, users_content};
use diesel::prelude::*;
use diesel_connection::get_connection;
use tokio::task::spawn_blocking;
pub struct MutationRoot;

#[Object]
impl MutationRoot {
  async fn create_user(&self, _ctx: &Context<'_>, data: CreateUser) -> FieldResult<User> {
    let user: Result<User, SimpleDieselError> = spawn_blocking(|| {
      let conn = get_connection()?;
      let user = diesel::insert_into(users::table)
        .values(data)
        .returning((users::id, users::name))
        .get_result::<User>(&conn)?;

      Ok(user)
    })
    .await
    .unwrap();

    user.map_err(|err| err.extend())
  }

  async fn create_content(&self, _ctx: &Context<'_>, data: CreateContent) -> FieldResult<Content> {
    let user: Result<Content, SimpleDieselError> = spawn_blocking(|| {
      let conn = get_connection()?;
      let user = diesel::insert_into(content::table)
        .values(data)
        .returning((content::id, content::title))
        .get_result::<Content>(&conn)?;

      Ok(user)
    })
    .await
    .unwrap();

    user.map_err(|err| err.extend())
  }

  async fn create_bookmark(
    &self,
    _ctx: &Context<'_>,
    user_id: UserId,
    content_id: ContentId,
  ) -> FieldResult<Bookmark> {
    let bookmark: Result<Bookmark, SimpleDieselError> = spawn_blocking(move || {
      let conn = get_connection()?;

      diesel::insert_into(users_content::table)
        .values((
          users_content::user_id.eq(user_id),
          users_content::content_id.eq(content_id),
        ))
        .on_conflict_do_nothing()
        .execute(&conn)?;

      let bookmark = users_content::table
        .filter(
          users_content::user_id
            .eq(user_id)
            .and(users_content::content_id.eq(content_id)),
        )
        .inner_join(content::table)
        .select((users_content::user_id, content::all_columns))
        .get_result::<Bookmark>(&conn)?;

      Ok(bookmark)
    })
    .await
    .unwrap();

    bookmark.map_err(|err| err.extend())
  }

  async fn delete_bookmark(
    &self,
    _ctx: &Context<'_>,
    user_id: i32,
    content_id: i32,
  ) -> FieldResult<DeletionReceipt> {
    let receipt: Result<DeletionReceipt, SimpleDieselError> = spawn_blocking(move || {
      let conn = get_connection()?;

      let id = diesel::delete(
        users_content::table.filter(
          users_content::user_id
            .eq(user_id)
            .and(users_content::content_id.eq(content_id)),
        ),
      )
      .returning(users_content::content_id)
      .get_result::<i32>(&conn)?;

      Ok(DeletionReceipt { id })
    })
    .await
    .unwrap();

    receipt.map_err(|err| err.extend())
  }
}
