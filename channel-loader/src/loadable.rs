use crate::task::TaskHandler;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Loadable types load using the corresponding static loader associated by type via [`define_static_loader`]
#[async_trait::async_trait]
pub trait Loadable<T: TaskHandler> {
  /// Load a value by it's key in a batched load. If no [`TaskHandler`] is pending assignment, one will be scheduled. Even though this is scheduled up front, task assignment is deferred and will capture all loads that come thereafter; for a given request, it is guaranteed all loads will be enqueued before task assigment and batched optimally.
  async fn load_by(key: T::Key) -> Result<Option<Arc<T::Value>>, T::Error>
  where
    T: TaskHandler;
}

/// Defines a static `DataLoader` instance from a type that implements [`TaskHandler`]
#[macro_export]
macro_rules! define_static_loader {
  ($loader:ty) => {
    #[static_init::dynamic(0)]
    static WORKER_REGISTRY: $crate::worker::WorkerRegistry<$loader> = $crate::worker::WorkerRegistry::new();

    thread_local! {
      static LOADER: $crate::loader::DataLoader<$loader> = $crate::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
    }

    impl $crate::loader::LocalLoader for $loader {
      fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
        &LOADER
      }
    }
  };

  ($name_prefix:ident, $loader:ty) => {
    $crate::paste::paste! {
      #[static_init::dynamic(0)]
      static [<$name_prefix _WORKER_REGISTRY>]: $crate::worker::WorkerRegistry<$loader> = $crate::worker::WorkerRegistry::new();

      thread_local! {
        static [<$name_prefix _LOADER>]: $crate::loader::DataLoader<$loader> = $crate::loader::DataLoader::from_registry(unsafe { &[<$name_prefix _WORKER_REGISTRY>] });
      }

      impl $crate::loader::LocalLoader for $loader {
        fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
          &[<$name_prefix _LOADER>]
        }
      }
    }
  };
}

/// Implements [`Loadable`] using the corresponding static instance as defined by [`define_static_loader`]
#[macro_export]
macro_rules! attach_loader {
  ($loadable:ty, $loader:ty) => {
    #[$crate::async_trait::async_trait]
    impl $crate::loadable::Loadable<$loader> for $loadable {
      async fn load_by(
        key: <$loader as $crate::task::TaskHandler>::Key,
      ) -> Result<
        Option<std::sync::Arc<<$loader as $crate::task::TaskHandler>::Value>>,
        <$loader as $crate::task::TaskHandler>::Error,
      > {
        use $crate::loader::LocalLoader;

        let rx = <$loader as LocalLoader>::loader().with(|loader| loader.load_by(key));

        rx.await.unwrap()
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

  #[derive(Default)]
  pub struct BatchLoader {}

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl TaskHandler for BatchLoader {
    type Key = i32;
    type Value = BatchSize;
    type Error = ();

    async fn handle_task(
      task: Task<PendingAssignment<BatchLoader>>,
    ) -> Task<CompletionReceipt<BatchLoader>> {
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

  define_static_loader!(BATCH, BatchLoader);
  attach_loader!(BatchSize, BatchLoader);

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
