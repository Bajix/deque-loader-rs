use crate::{
  request::{LoadCache, LoadState, Request},
  task::{Task, TaskHandler},
  worker::{QueueHandle, WorkerRegistry},
};
use crossbeam::deque::Worker;
use std::{sync::Arc, thread::LocalKey};
use tokio::sync::{oneshot, watch};
/// Each DataLoader is a thread local owner of a  [`crossbeam::deque::Worker`] deque for a given worker group
pub struct DataLoader<T: TaskHandler> {
  queue: Worker<Request<T>>,
  queue_handle: &'static QueueHandle<T>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new(queue: Worker<Request<T>>, queue_handle: &'static QueueHandle<T>) -> Self {
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

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::<T>::new_oneshot(key);

    if self.queue_handle.queue_size.fetch_add(1).eq(&0) {
      let task = Task::new(self.queue_handle);
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    self.queue.push(req);

    rx
  }

  pub fn cached_load_by(&self, key: T::Key, cache: &LoadCache<T>) -> watch::Receiver<LoadState<T>>
  where
    T: TaskHandler,
  {
    let (rx, req) = cache.get_or_create(&key);

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
}

pub trait LocalLoader {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
}