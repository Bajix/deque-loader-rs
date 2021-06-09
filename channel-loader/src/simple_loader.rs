use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use std::{collections::HashMap, marker::PhantomData};

#[async_trait::async_trait]
pub trait SimpleWorker: Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  async fn load(keys: Vec<Self::Key>) -> Result<HashMap<Self::Key, Self::Value>, Self::Error>;
}

pub struct SimpleLoader<T: SimpleWorker> {
  worker: PhantomData<fn() -> T>,
}

impl<T> Default for SimpleLoader<T>
where
  T: SimpleWorker,
{
  fn default() -> Self {
    SimpleLoader {
      worker: PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<T> TaskHandler for SimpleLoader<T>
where
  T: SimpleWorker + 'static,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = T::Error;
  const MAX_BATCH_SIZE: i32 = T::MAX_BATCH_SIZE;

  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>> {
    match task.get_assignment() {
      TaskAssignment::LoadBatch(task) => {
        let keys = task.keys();
        let result = T::load(keys).await;
        task.resolve(result)
      }
      TaskAssignment::NoAssignment(receipt) => receipt,
    }
  }
}
