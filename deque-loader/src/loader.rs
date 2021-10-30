use crate::{
  request::{ContextCache, OneshotReceiver, Request, WatchReceiver},
  task::{CompletionReceipt, LoadBatch, PendingAssignment, Task, TaskHandler},
};
use std::thread::LocalKey;
use swap_queue::Worker;

/// Each DataLoader is a thread local owner of a [`swap_queue::Worker`] queue for a given worker group
pub struct DataLoader<T: TaskHandler> {
  queue: Worker<Request<T::Key, T::Value, T::Error>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new(queue: Worker<Request<T::Key, T::Value, T::Error>>) -> Self {
    DataLoader { queue }
  }

  pub fn load_by(&self, key: T::Key) -> OneshotReceiver<T::Value, T::Error> {
    let (req, rx) = Request::new_oneshot(key);

    if let Some(stealer) = self.queue.push(req) {
      let task = Task::new(stealer);
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    rx
  }

  pub fn cached_load_by<RequestCache: Send + Sync + AsRef<ContextCache<T>>>(
    &self,
    key: T::Key,
    request_cache: &RequestCache,
  ) -> WatchReceiver<T::Value, T::Error> {
    let (rx, req) = request_cache.as_ref().get_or_create(&key);

    if let Some(req) = req {
      if let Some(stealer) = self.queue.push(req) {
        let task = Task::new(stealer);
        tokio::task::spawn(async move {
          T::handle_task(task).await;
        });
      }
    }

    rx
  }

  pub fn schedule_assignment(
    &self,
    task: Task<LoadBatch<T::Key, T::Value, T::Error>>,
  ) -> Task<CompletionReceipt> {
    let Task(LoadBatch { requests }) = task;

    let mut requests = requests.into_iter();

    while let Some(req) = requests.next() {
      if let Some(stealer) = self.queue.push(req) {
        let task = Task(PendingAssignment {
          stealer,
          requests: requests.collect(),
        });

        tokio::task::spawn(async move {
          T::handle_task(task).await;
        });

        return Task::completion_receipt();
      }
    }

    Task::completion_receipt()
  }
}

impl<T> Default for DataLoader<T>
where
  T: TaskHandler,
{
  fn default() -> Self {
    let queue = Worker::new();
    DataLoader { queue }
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
