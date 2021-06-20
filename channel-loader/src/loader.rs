use crate::{
  request::Request,
  task::{Task, TaskHandler},
};
use crossbeam::{atomic::AtomicCell, deque::Worker};
use std::{sync::Arc, thread::LocalKey};
use tokio::sync::oneshot;

struct Stealers {}

/// Core load channel responsible for receiving incoming load_by requests to be processed by a detached [`RequestReactor`]
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) load_capacity: Arc<AtomicCell<i32>>,
  pub(crate) queue: Worker<Request<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    let load_capacity = Arc::new(AtomicCell::new(0));
    let queue = Worker::new_fifo();

    Self {
      load_capacity,
      queue,
    }
  }

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::new(key);

    self.queue.push(req);

    if self.load_capacity.fetch_sub(1).eq(&0) {
      let task = Task::new(self.load_capacity.clone(), self.queue.stealer());
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    rx
  }
}

pub trait StaticLoaderExt {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
}
