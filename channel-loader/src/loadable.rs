use crate::task::TaskHandler;
use tokio::sync::oneshot;

/// Loadable types load using the corresponding static loader associated by type via [`define_static_loader`]
pub trait Loadable<T: TaskHandler> {
  /// Load a value by it's key in a batched load to be schedule after the next [`TaskHandler`] is ready for a task assignment, typically after yielding to [`tokio`] and a connection has been acquired
  fn load_by(key: T::Key) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    T: TaskHandler;
}

/// Defines a static `DataLoader` instance from a type that implements [`TaskHandler`] and registers reactor initialization to be handled via [`booter::boot()`];
#[macro_export]
macro_rules! define_static_loader {
  ($loader:ty) => {
    thread_local! {
      static LOADER: $crate::loader::DataLoader<$loader> = $crate::loader::DataLoader::new();
    }

    impl $crate::loader::StaticLoaderExt for $loader {
      fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
        &LOADER
      }
    }
  };

  ($loader_name:ident, $loader:ty) => {
    thread_local! {
      static $loader_name: $crate::loader::DataLoader<$loader> =
      $crate::loader::DataLoader::new();
    }

    impl $crate::loader::StaticLoaderExt for $loader {
      fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
        &$loader_name
      }
    }
  };
}

/// Implements [`Loadable`] using the corresponding static instance as defined by [`define_static_loader`]
#[macro_export]
macro_rules! attach_loader {
  ($loadable:ty, $loader:ty) => {
    impl $crate::loadable::Loadable<$loader> for $loadable {
      fn load_by(
        key: <$loader as $crate::task::TaskHandler>::Key,
      ) -> tokio::sync::oneshot::Receiver<
        Result<
          Option<<$loader as $crate::task::TaskHandler>::Value>,
          <$loader as $crate::task::TaskHandler>::Error,
        >,
      > {
        use $crate::loader::StaticLoaderExt;

        <$loader as StaticLoaderExt>::loader().with(|loader| loader.load_by(key))
      }
    }
  };
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment};
  use std::{collections::HashMap, iter};

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
          let mut data: HashMap<i32, BatchSize> = HashMap::new();
          let keys = task.keys();

          data.extend(
            task
              .keys()
              .into_iter()
              .zip(iter::repeat(BatchSize(keys.len()))),
          );

          task.resolve(Ok(data))
        }
        TaskAssignment::NoAssignment(receipt) => receipt,
      }
    }
  }

  define_static_loader!(BATCH_LOADER, BatchLoader);
  attach_loader!(BatchSize, BatchLoader);

  #[tokio::test]
  async fn it_loads() -> Result<(), ()> {
    let data = BatchSize::load_by(1_i32).await.unwrap()?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_auto_batches() -> Result<(), ()> {
    let a = BatchSize::load_by(2_i32);

    let _b = BatchSize::load_by(3_i32);

    let data = a.await.unwrap()?;

    if let Some(BatchSize(n)) = data {
      assert!(n.ge(&2));
    } else {
      panic!("Request failed to batch");
    }

    Ok(())
  }
}
