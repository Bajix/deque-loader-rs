use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task},
};

#[async_trait::async_trait]
pub trait TaskHandler<K: Key>: Default + Send + Sync {
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  async fn handle_task(task: Task<PendingAssignment<K, Self>>) -> Task<CompletionReceipt<K, Self>>;
}
