use crate::{
  loader::{DataLoader, LocalLoader},
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
  Key,
};
use std::{collections::HashMap, sync::Arc};

/// Simplified TaskHandler interface
#[async_trait::async_trait]
pub trait BatchLoader: Sized + Send + Sync + 'static {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  type Error: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  async fn load(keys: Vec<Self::Key>) -> Result<HashMap<Self::Key, Arc<Self::Value>>, Self::Error>;
}

pub struct BatchHandler<T: BatchLoader>(T);

#[async_trait::async_trait]
impl<T> TaskHandler for BatchHandler<T>
where
  T: BatchLoader,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = T::Error;
  const CORES_PER_WORKER_GROUP: usize = T::CORES_PER_WORKER_GROUP;

  async fn handle_task(
    task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
  ) -> Task<CompletionReceipt> {
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

impl<Loader> LocalLoader for BatchHandler<Loader>
where
  Loader: BatchLoader + LocalLoader,
{
  type Handler = <Loader as LocalLoader>::Handler;
  fn loader() -> &'static std::thread::LocalKey<DataLoader<Self::Handler>> {
    Loader::loader()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::LoadBy;
  use deque_loader_derive::{Loadable, Loader};
  use std::{collections::HashMap, iter};
  use tokio::try_join;

  #[derive(Loader)]
  #[data_loader(handler = "BatchHandler<BatchSizeLoader>", internal = true)]
  pub struct BatchSizeLoader {}

  #[derive(Clone, Debug, PartialEq, Eq, Loadable)]
  #[data_loader(handler = "BatchHandler<BatchSizeLoader>", internal = true)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl BatchLoader for BatchSizeLoader {
    type Key = i32;
    type Value = BatchSize;
    type Error = ();
    async fn load(
      keys: Vec<Self::Key>,
    ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, Self::Error> {
      let mut data: HashMap<i32, Arc<BatchSize>> = HashMap::new();
      let len = keys.len();
      data.extend(keys.into_iter().zip(iter::repeat(Arc::new(BatchSize(len)))));

      Ok(data)
    }
  }

  #[tokio::test]
  async fn it_loads() -> Result<(), ()> {
    let data = BatchSize::load_by(1_i32).await?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_auto_batches() -> Result<(), ()> {
    let a = BatchSize::load_by(2_i32);

    let b = BatchSize::load_by(3_i32);

    let (a, b) = try_join!(a, b)?;

    assert_eq!(a, b);
    assert!(a.unwrap().0.ge(&2));

    Ok(())
  }
}
