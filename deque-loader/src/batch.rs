use crate::{
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

/// Setup thread local [`DataLoader`] instances using a [`BatchLoader`] to define the [`TaskHandler`]
#[async_trait::async_trait]
impl<T> TaskHandler for T
where
  T: BatchLoader,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = T::Error;
  const CORES_PER_WORKER_GROUP: usize = T::CORES_PER_WORKER_GROUP;

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

#[cfg(test)]
mod tests {
  use crate::{attach_loader, Loadable};

  use super::*;
  use std::{collections::HashMap, iter};
  use tokio::try_join;
  pub struct Loader {}

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl BatchLoader for Loader {
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

  define_static_loader!(Loader);
  attach_loader!(BatchSize, Loader);

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
