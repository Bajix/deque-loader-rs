use async_graphql::{InputObject, SimpleObject};
use db::schema::users;

#[derive(InputObject, Insertable)]
#[table_name = "users"]
pub struct CreateUser {
  pub name: String,
}

#[derive(SimpleObject, Queryable)]
pub struct User {
  pub id: i32,
  pub name: String,
}
