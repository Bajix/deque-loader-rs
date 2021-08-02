use super::{Content, User, UserId};
use async_graphql::SimpleObject;
use db::schema::{content, users_content};
use deque_loader::diesel::{DieselError, DieselHandler, DieselLoader};
use diesel::prelude::*;
use diesel_connection::PooledConnection;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(
  SimpleObject,
  Identifiable,
  Queryable,
  Associations,
  Clone,
  Loadable,
  Serialize,
  Deserialize,
  Debug,
)]
#[belongs_to(UserId, foreign_key = "user_id")]
#[belongs_to(User, foreign_key = "user_id")]
#[table_name = "users_content"]
#[primary_key("user_id")]
#[cached_loader(handler = "DieselHandler<BookmarkLoader>")]
pub struct Bookmark {
  pub user_id: i32,
  #[diesel(embed)]
  pub content: Content,
}

#[derive(Loader)]
#[cached_loader(handler = "DieselHandler<BookmarkLoader>")]
pub struct BookmarkLoader;

impl DieselLoader for BookmarkLoader {
  type Key = UserId;
  type Value = Vec<Bookmark>;
  fn load(
    conn: PooledConnection,
    keys: Vec<UserId>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError> {
    let bookmarks: Vec<Bookmark> = Bookmark::belonging_to(&keys)
      .inner_join(content::table)
      .select((users_content::user_id, content::all_columns))
      .load::<Bookmark>(&conn)?;

    let grouped_bookmarks = bookmarks.grouped_by(&keys).into_iter().map(Arc::new);

    let mut data: HashMap<UserId, Arc<Vec<Bookmark>>> = HashMap::new();

    data.extend(keys.into_iter().zip(grouped_bookmarks));

    Ok(data)
  }
}
