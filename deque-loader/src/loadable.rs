use crate::{request::LoadCache, task::TaskHandler, Key};
use std::sync::Arc;

#[async_trait::async_trait]
/// Use [`attach_handler`] to define
pub trait Loadable<T, K, V>
where
  T: TaskHandler,
  K: Key,
  V: Send + Sync + Clone + 'static,
{
  type Error: Send + Sync + Clone + 'static;
  /// Load a value by it's key in a batched load. If no [`TaskHandler`] is pending assignment, one will be scheduled. Even though this is scheduled up front, task assignment is deferred and will capture all loads that come thereafter; for a given request, it is guaranteed all loads will be enqueued before task assigment and batched optimally.
  async fn load_by(key: K) -> Result<Option<Arc<V>>, Self::Error>;
  /// Load against a request contextual cache. Use [`register_cache_factory`] and [`crate::graphql::insert_loader_caches`] to hydrate Context<'_> and to define AsRef impl
  async fn cached_load_by<Cache: Send + AsRef<LoadCache<T>>>(
    key: K,
    cache: Cache,
  ) -> Result<Option<Arc<V>>, Self::Error>;
}

/// Thread local [`DataLoader`] instances grouped into worker groups and statically pre-allocated per core as to be lock free
#[macro_export]
macro_rules! define_static_loader {
  ($loader:ty, $handler:ty) => {
    impl $crate::loader::LocalLoader for $loader {
      type Handler = $handler;
      fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<Self::Handler>> {
        #[static_init::dynamic(0)]
        static WORKER_REGISTRY: $crate::worker::WorkerRegistry<$handler> = $crate::worker::WorkerRegistry::new();

        thread_local! {
          static DATA_LOADER: $crate::loader::DataLoader<$handler> = $crate::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
        }

        &DATA_LOADER
      }
    }
  };
}

/// Implements [`Loadable`] using the current thread local [`DataLoader`]
#[macro_export]
macro_rules! attach_handler {
  ($loadable:ty, $handler:ty) => {
    #[$crate::async_trait::async_trait]
    impl
      $crate::loadable::Loadable<
        $handler,
        <$handler as $crate::task::TaskHandler>::Key,
        <$handler as $crate::task::TaskHandler>::Value,
      > for $loadable
    {
      type Error = <$handler as $crate::task::TaskHandler>::Error;
      async fn load_by(key: <$handler as $crate::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<$handler as $crate::task::TaskHandler>::Value>>, Self::Error> {
        use $crate::loader::LocalLoader;

        let rx = <$handler as LocalLoader>::loader().with(|loader| loader.load_by(key));

        rx.recv().await
      }

      async fn cached_load_by<Cache: Send + AsRef<$crate::request::LoadCache<$handler>>>(
        key: <$handler as $crate::task::TaskHandler>::Key,
        cache: Cache,
      ) -> Result<Option<std::sync::Arc<<$handler as $crate::task::TaskHandler>::Value>>, Self::Error> {
        use $crate::loader::LocalLoader;

        let rx =
          <$handler as LocalLoader>::loader().with(|loader| loader.cached_load_by(key, cache));

        rx.recv().await
      }
    }
  };
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment};
  use std::{collections::HashMap, iter};
  use tokio::try_join;
  pub struct BatchLoader {}

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl TaskHandler for BatchLoader {
    type Key = i32;
    type Value = BatchSize;
    type Error = ();
    async fn handle_task(task: Task<PendingAssignment<BatchLoader>>) -> Task<CompletionReceipt> {
      match task.get_assignment() {
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

  define_static_loader!(BatchLoader, BatchLoader);
  attach_handler!(BatchSize, BatchLoader);

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
