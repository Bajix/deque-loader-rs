use super::Bookmark;
use async_graphql::{
  ComplexObject, Context, ErrorExtensions, FieldResult, InputObject, SimpleObject,
};
use db::schema::users;
use deque_loader::{
  diesel::{DieselError, DieselHandler, DieselLoader},
  LoadBy,
};
use diesel::prelude::*;
use diesel_connection::PooledConnection;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(InputObject, Insertable)]
#[diesel(table_name = users)]
pub struct CreateUser {
  pub name: String,
}

derive_id! {
  #[derive(Identifiable)]
  #[diesel(table_name = users)]
  #[graphql(name = "UserID")]
  pub struct UserId( #[diesel(column_name = "id")] i32);
}

#[derive(
  SimpleObject,
  Identifiable,
  Associations,
  Queryable,
  Debug,
  Clone,
  Loadable,
  Serialize,
  Deserialize,
)]
#[graphql(complex)]
#[diesel(belongs_to(UserId, foreign_key = id))]
#[diesel(table_name = users)]
#[data_loader(handler = "DieselHandler<UsersLoader>")]
#[data_loader(handler = "DieselHandler<UserLoader>")]
pub struct User {
  pub id: i32,
  pub name: String,
}

#[ComplexObject]
impl User {
  async fn bookmarks(&self, _ctx: &Context<'_>) -> FieldResult<Arc<Vec<Bookmark>>> {
    let bookmarks = Bookmark::load_by(UserId::from(self.id))
      .await
      .map_err(|err| err.extend())?
      .unwrap_or_else(|| Arc::new(vec![]));

    Ok(bookmarks)
  }
}
#[derive(Loader)]
#[data_loader(handler = "DieselHandler<UserLoader>")]
pub struct UserLoader;

impl DieselLoader for UserLoader {
  type Key = UserId;
  type Value = User;
  fn load(
    mut conn: PooledConnection,
    keys: Vec<UserId>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError> {
    let users: Vec<User> = User::belonging_to(&keys)
      .select(users::all_columns)
      .load::<User>(&mut conn)?;

    let users = users.into_iter().map(Arc::new);

    let mut data: HashMap<UserId, Arc<User>> = HashMap::new();

    data.extend(keys.into_iter().zip(users));

    Ok(data)
  }
}

#[derive(Loader)]
#[data_loader(handler = "DieselHandler<UsersLoader>")]
pub struct UsersLoader;

impl DieselLoader for UsersLoader {
  type Key = ();
  type Value = Vec<User>;
  fn load(
    mut conn: PooledConnection,
    _: Vec<()>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError> {
    let users = users::table
      .limit(50)
      .select(users::all_columns)
      .get_results::<User>(&mut conn)?;

    let mut data: HashMap<(), Arc<Vec<User>>> = HashMap::new();

    data.insert((), Arc::new(users));

    Ok(data)
  }
}
