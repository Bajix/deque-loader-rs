use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use std::{collections::HashMap, marker::PhantomData};

#[async_trait::async_trait]
pub trait SimpleWorker<K: Key>: Send + Sync {
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  async fn load(keys: Vec<K>) -> Result<HashMap<K, Self::Value>, Self::Error>;
}

pub struct SimpleLoader<K: Key, T: SimpleWorker<K>> {
  key: PhantomData<fn() -> K>,
  worker: PhantomData<fn() -> T>,
}

impl<K, T> Default for SimpleLoader<K, T>
where
  K: Key,
  T: SimpleWorker<K>,
{
  fn default() -> Self {
    SimpleLoader {
      key: PhantomData,
      worker: PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<K, T> TaskHandler<K> for SimpleLoader<K, T>
where
  K: Key,
  T: SimpleWorker<K> + 'static,
{
  type Value = T::Value;
  type Error = T::Error;
  const MAX_BATCH_SIZE: i32 = T::MAX_BATCH_SIZE;

  async fn handle_task(task: Task<PendingAssignment<K, Self>>) -> Task<CompletionReceipt<K, Self>> {
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
