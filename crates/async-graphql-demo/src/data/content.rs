use async_graphql::{InputObject, SimpleObject};
use db::schema::content;

#[derive(InputObject, Insertable)]
#[table_name = "content"]
pub struct CreateContent {
  pub title: String,
}

#[derive(SimpleObject, Queryable)]
pub struct Content {
  pub id: i32,
  pub title: String,
}
