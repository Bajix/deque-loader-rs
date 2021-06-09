use crate::{
  reactor::{ReactorSignal, RequestReactor},
  request::Request,
  task::TaskHandler,
};
use atomic_take::AtomicTake;
use flume::{self, Sender};
use tokio::sync::oneshot;

/// Core load channel responsible for receiving incoming load_by requests to be processed by a detached [`RequestReactor`]
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) tx: Sender<ReactorSignal<T>>,
  reactor: AtomicTake<RequestReactor<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    let (tx, rx) = flume::unbounded::<ReactorSignal<T>>();
    let reactor = AtomicTake::new(RequestReactor::new(rx));
    Self { tx, reactor }
  }

  // Start processing incoming load requests in the background
  pub fn start_detached_reactor(&self) {
    if let Some(reactor) = self.reactor.take() {
      reactor.start_detached();
    }
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

pub trait StaticLoaderExt<T: TaskHandler> {
  fn loader() -> &'static DataLoader<T>;
}
