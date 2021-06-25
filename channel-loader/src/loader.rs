use crate::{
  request::Request,
  task::{Task, TaskHandler},
  worker::{QueueHandle, WorkerRegistry},
};
use crossbeam::deque::Worker;
use std::{sync::Arc, thread::LocalKey};
use tokio::sync::oneshot;

/// Core load channel responsible for receiving incoming load_by requests to be enqueued via thread local [`crossbeam::deque::Worker`] queues
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) queue: Worker<Request<T>>,
  pub(crate) queue_handle: Arc<QueueHandle<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    let queue = Worker::new_fifo();
    let queue_handle = Arc::new(QueueHandle::new(vec![queue.stealer()]));

    Self {
      queue_handle,
      queue,
    }
  }

  pub fn from_registry(registry: &'static WorkerRegistry<T>) -> Self {
    registry
      .create_local_loader()
      .expect("There can only be at most one thread local DataLoader per CPU core")
  }

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::<T>::new(key);

    if self.queue_handle.queue_size.fetch_add(1).eq(&0) {
      let task = Task::new(self.queue_handle.clone());
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    self.queue.push(req);

    rx
  }
}

pub trait LocalLoader {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
}
