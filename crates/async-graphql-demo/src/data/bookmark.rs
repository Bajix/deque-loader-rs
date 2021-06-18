use super::{Content, User, UserId};
use async_graphql::SimpleObject;
use channel_loader::diesel::{DieselError, DieselLoader};
use db::schema::{content, users_content};
use diesel::prelude::*;
use diesel_connection::PooledConnection;
use std::collections::HashMap;

#[derive(SimpleObject, Identifiable, Queryable, Associations, Clone)]
#[belongs_to(UserId, foreign_key = "user_id")]
#[belongs_to(User, foreign_key = "user_id")]
#[table_name = "users_content"]
#[primary_key("user_id")]
pub struct Bookmark {
  pub user_id: i32,
  #[diesel(embed)]
  pub content: Content,
}

#[derive(Default)]
pub struct BookmarkLoader;

impl DieselLoader for BookmarkLoader {
  type Key = UserId;
  type Value = Vec<Bookmark>;
  const MAX_BATCH_SIZE: i32 = 500;
  fn load(
    conn: PooledConnection,
    keys: Vec<UserId>,
  ) -> Result<HashMap<Self::Key, Self::Value>, DieselError> {
    let bookmarks: Vec<Bookmark> = Bookmark::belonging_to(&keys)
      .inner_join(content::table)
      .select((users_content::user_id, content::all_columns))
      .load::<Bookmark>(&conn)?;

    let grouped_bookmarks = bookmarks.grouped_by(&keys);

    let mut data: HashMap<UserId, Vec<Bookmark>> = HashMap::new();

    data.extend(keys.into_iter().zip(grouped_bookmarks.into_iter()));

    Ok(data)
  }
}

define_static_loader!(BookmarkLoader);
attach_loader!(Bookmark, BookmarkLoader);
