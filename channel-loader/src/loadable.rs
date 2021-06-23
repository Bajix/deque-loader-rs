use crate::task::TaskHandler;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Loadable types load using the corresponding static loader associated by type via [`define_static_loader`]
pub trait Loadable<T: TaskHandler> {
  /// Load a value by it's key in a batched load. If no [`TaskHandler`] is pending assignment, one will be scheduled. Even though this is scheduled up front, task assignment is deferred and will capture all loads that come thereafter; for a given request, it is guaranteed all loads will be enqueued before task assigment and batched optimally.
  fn load_by(key: T::Key) -> oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>
  where
    T: TaskHandler;
}

/// Defines a static `DataLoader` instance from a type that implements [`TaskHandler`]
#[macro_export]
macro_rules! define_static_loader {
  ($loader:ty) => {
    static QUEUE_SIZE: $crate::crossbeam::atomic::AtomicCell<i32> = $crate::crossbeam::atomic::AtomicCell::new(0);

    #[static_init::dynamic(0)]
    static TASK_STEALERS: std::sync::RwLock<Vec<$crate::crossbeam::deque::Stealer<$crate::request::Request<$loader>>>> = std::sync::RwLock::new(Vec::new());

    thread_local! {
      static LOADER: $crate::loader::DataLoader<$loader> = $crate::loader::DataLoader::new(&QUEUE_SIZE, unsafe { &TASK_STEALERS });
    }

    impl $crate::loader::StaticLoaderExt for $loader {
      fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
        &LOADER
      }
      fn queue_size() -> &'static $crate::crossbeam::atomic::AtomicCell<i32> {
        &QUEUE_SIZE
      }
      fn task_stealers<'a>() -> std::sync::RwLockReadGuard<'a, Vec<$crate::crossbeam::deque::Stealer<$crate::request::Request<$loader>>>> {
        let stealers = unsafe {
          &TASK_STEALERS
        };
        stealers.read().unwrap()
      }
    }
  };

  ($name_prefix:ident, $loader:ty) => {
    $crate::paste::paste! {
      static [<$name_prefix _QUEUE_SIZE>]: $crate::crossbeam::atomic::AtomicCell<i32> = $crate::crossbeam::atomic::AtomicCell::new(0);

      #[static_init::dynamic(0)]
      static [<$name_prefix _TASK_STEALERS>]: std::sync::RwLock<Vec<$crate::crossbeam::deque::Stealer<$crate::request::Request<$loader>>>> = std::sync::RwLock::new(Vec::new());

      thread_local! {
        static [<$name_prefix _LOADER>]: $crate::loader::DataLoader<$loader> =
        $crate::loader::DataLoader::new(&[<$name_prefix _QUEUE_SIZE>], unsafe {&[<$name_prefix _TASK_STEALERS>]});
      }

      impl $crate::loader::StaticLoaderExt for $loader {
        fn loader() -> &'static std::thread::LocalKey<$crate::loader::DataLoader<$loader>> {
          &[<$name_prefix _LOADER>]
        }
        fn queue_size() -> &'static $crate::crossbeam::atomic::AtomicCell<i32> {
          &[<$name_prefix _QUEUE_SIZE>]
        }
        fn task_stealers<'a>() -> std::sync::RwLockReadGuard<'a, Vec<$crate::crossbeam::deque::Stealer<$crate::request::Request<$loader>>>> {
          let stealers = unsafe {
            &[<$name_prefix _TASK_STEALERS>]
          };
          stealers.read().unwrap()
        }
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
          Option<std::sync::Arc<<$loader as $crate::task::TaskHandler>::Value>>,
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
    let data = BatchSize::load_by(1_i32).await.unwrap()?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_auto_batches() -> Result<(), ()> {
    let a = BatchSize::load_by(2_i32);

    let _b = BatchSize::load_by(3_i32);

    let data = a.await.unwrap()?;

    if let Some(batch) = data {
      assert!(batch.0.ge(&2));
    } else {
      panic!("Request failed to batch");
    }

    Ok(())
  }
}
