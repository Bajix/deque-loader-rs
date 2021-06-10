use super::Content;
use async_graphql::SimpleObject;

#[derive(SimpleObject, Queryable)]
pub struct Bookmark {
  pub user_id: i32,
  #[diesel(embed)]
  pub content: Content,
}
