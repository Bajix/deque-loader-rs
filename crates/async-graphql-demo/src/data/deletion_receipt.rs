use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct DeletionReceipt {
  pub id: i32,
}
