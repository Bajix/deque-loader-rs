use async_graphql::Object;

#[derive(Default)]
pub struct QueryRoot;

#[Object]
impl QueryRoot {
  async fn status(&self) -> i32 {
    200
  }
}
