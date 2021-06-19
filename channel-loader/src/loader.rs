use std::thread::LocalKey;

use crate::{
  reactor::{ReactorSignal, RequestReactor},
  request::Request,
  task::TaskHandler,
};
use flume::{self, Sender};
use tokio::sync::oneshot;

/// Core load channel responsible for receiving incoming load_by requests to be processed by a detached [`RequestReactor`]
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) tx: Sender<ReactorSignal<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    let (tx, rx) = flume::unbounded::<ReactorSignal<T>>();
    let reactor = RequestReactor::new(rx);
    reactor.start_detached();
    Self { tx }
  }

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::new(key);

    self.tx.send(ReactorSignal::Load(req)).ok();

    rx
  }
}

pub trait StaticLoaderExt {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
}
