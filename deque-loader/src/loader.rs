use crate::{
  request::{LoadCache, OneshotReceiver, Request, WatchReceiver},
  task::{CompletionReceipt, LoadBatch, PendingAssignment, Task, TaskHandler},
  worker::{QueueHandle, WorkerRegistry},
};
use crossbeam::deque::Worker;
use std::thread::LocalKey;

/// Each DataLoader is a thread local owner of a  [`crossbeam::deque::Worker`] deque for a given worker group
pub struct DataLoader<T: TaskHandler> {
  queue: Worker<Request<T::Key, T::Value, T::Error>>,
  queue_handle: &'static QueueHandle<T::Key, T::Value, T::Error>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new(
    queue: Worker<Request<T::Key, T::Value, T::Error>>,
    queue_handle: &'static QueueHandle<T::Key, T::Value, T::Error>,
  ) -> Self {
    DataLoader {
      queue,
      queue_handle,
    }
  }

  pub fn from_registry(registry: &'static WorkerRegistry<T>) -> Self {
    registry
      .take_loader()
      .expect("There can only be at most one thread local DataLoader per CPU core")
  }

  pub fn load_by(&self, key: T::Key) -> OneshotReceiver<T::Value, T::Error> {
    let (req, rx) = Request::new_oneshot(key);

    if self.queue_handle.queue_size.fetch_add(1).eq(&0) {
      let task = Task::new(self.queue_handle);
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    self.queue.push(req);

    rx
  }

  pub fn cached_load_by<RequestCache: Send + Sync + AsRef<LoadCache<T>>>(
    &self,
    key: T::Key,
    request_cache: &RequestCache,
  ) -> WatchReceiver<T::Value, T::Error> {
    let (rx, req) = request_cache.as_ref().get_or_create(&key);

    if let Some(req) = req {
      if self.queue_handle.queue_size.fetch_add(1).eq(&0) {
        let task = Task::new(self.queue_handle);
        tokio::task::spawn(async move {
          T::handle_task(task).await;
        });
      }

      self.queue.push(req);
    }

    rx
  }

  pub fn schedule_assignment(
    &self,
    task: Task<LoadBatch<T::Key, T::Value, T::Error>>,
  ) -> Task<CompletionReceipt> {
    let Task(LoadBatch { requests }) = task;

    let task = Task(PendingAssignment {
      queue_handle: self.queue_handle,
      requests,
    });

    tokio::task::spawn(async move {
      T::handle_task(task).await;
    });

    Task::completion_receipt()
  }
}

pub trait StoreType {}
pub struct CacheStore;
impl StoreType for CacheStore {}
pub struct DataStore;
impl StoreType for DataStore {}
pub trait LocalLoader<T: StoreType>: Sized + Send + Sync + 'static {
  type Handler: TaskHandler;
  fn loader() -> &'static LocalKey<DataLoader<Self::Handler>>;
}
