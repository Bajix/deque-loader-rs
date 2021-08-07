use crate::{request::LoadCache, task::TaskHandler, Key};
use std::sync::Arc;

#[async_trait::async_trait]
/// Derive with [`deque_loader_derive::Loadable`]
pub trait LoadBy<T, K, V>
where
  T: TaskHandler,
  K: Key,
  V: Send + Sync + Clone + 'static,
{
  type Error: Send + Sync + Clone + 'static;
  /// Load a value by it's key in a batched load. If no [`TaskHandler`] is pending assignment, one will be scheduled. Even though this is scheduled up front, task assignment is deferred and will capture all loads that come thereafter; for a given request, it is guaranteed all loads will be enqueued before task assigment and batched optimally.
  async fn load_by(key: K) -> Result<Option<Arc<V>>, Self::Error>;
  /// Load against a request contextual cache. Use [`register_cache_factory`] and [`crate::graphql::insert_loader_caches`] to hydrate Context<'_> and to define AsRef impl
  async fn cached_load_by<RequestCache: Send + Sync + AsRef<LoadCache<T>>>(
    key: K,
    request_cache: &RequestCache,
  ) -> Result<Option<Arc<V>>, Self::Error>;
}

#[cfg(test)]
mod tests {
  use crate::{
    loadable::LoadBy,
    task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
  };
  use deque_loader_derive::{Loadable, Loader};
  use std::{collections::HashMap, iter, sync::Arc};
  use tokio::try_join;

  #[derive(Loader)]
  #[data_loader(handler = "BatchLoader", internal = true)]
  pub struct BatchLoader {}

  #[derive(Clone, Debug, PartialEq, Eq, Loadable)]
  #[data_loader(handler = "BatchLoader", internal = true)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl TaskHandler for BatchLoader {
    type Key = i32;
    type Value = BatchSize;
    type Error = ();
    async fn handle_task(
      task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
    ) -> Task<CompletionReceipt> {
      match task.get_assignment::<Self>() {
        TaskAssignment::LoadBatch(task) => {
          let mut data: HashMap<i32, Arc<BatchSize>> = HashMap::new();
          let keys = task.keys();

          data.extend(
            task
              .keys()
              .into_iter()
              .zip(iter::repeat(Arc::new(BatchSize(keys.len())))),
          );

          task.resolve(Ok(data))
        }
        TaskAssignment::NoAssignment(receipt) => receipt,
      }
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
