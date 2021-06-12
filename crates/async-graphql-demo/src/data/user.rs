use async_graphql::{
  ComplexObject, Context, ErrorExtensions, FieldResult, InputObject, SimpleObject,
};
use channel_loader::{
  diesel::{DieselError, DieselLoader},
  Loadable,
};
use db::schema::users;
use diesel::prelude::*;
use diesel_connection::PooledConnection;
use std::collections::HashMap;

use super::Bookmark;

#[derive(InputObject, Insertable)]
#[table_name = "users"]
pub struct CreateUser {
  pub name: String,
}

derive_id! {
  #[derive(Identifiable)]
  #[table_name = "users"]
  #[graphql(name = "UserID")]
  pub struct UserId( #[column_name = "id"] i32);
}

#[derive(SimpleObject, Identifiable, Associations, Queryable, Debug, Clone)]
#[graphql(complex)]
#[belongs_to(UserId, foreign_key = "id")]
#[table_name = "users"]
pub struct User {
  pub id: i32,
  pub name: String,
}

#[ComplexObject]
impl User {
  async fn bookmarks(&self, _ctx: &Context<'_>) -> FieldResult<Vec<Bookmark>> {
    let bookmarks = Bookmark::load_by(UserId::from(self.id))
      .await
      .unwrap()
      .map_err(|err| err.extend())?
      .unwrap_or_else(|| vec![]);

    Ok(bookmarks)
  }
}

#[derive(Default)]
pub struct UserLoader;

impl DieselLoader for UserLoader {
  type Key = UserId;
  type Value = Vec<User>;
  fn load(
    conn: PooledConnection,
    keys: Vec<UserId>,
  ) -> Result<HashMap<Self::Key, Self::Value>, DieselError> {
    let users: Vec<User> = User::belonging_to(&keys)
      .select(users::all_columns)
      .load::<User>(&conn)?;

    let grouped_users = users.grouped_by(&keys);

    let mut data: HashMap<UserId, Vec<User>> = HashMap::new();

    data.extend(keys.into_iter().zip(grouped_users.into_iter()));

    Ok(data)
  }
}

define_static_loader!(UserLoader);
attach_loader!(User, UserLoader);
