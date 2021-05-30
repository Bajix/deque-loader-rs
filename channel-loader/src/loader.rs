use crate::{
  key::Key,
  reactor::{ReactorSignal, RequestReactor},
  request::Request,
  task::TaskHandler,
};
use atomic_take::AtomicTake;
use flume::{self, Sender};
use tokio::sync::oneshot;

pub struct DataLoader<K: Key, T: TaskHandler<K> + 'static> {
  pub(crate) tx: Sender<ReactorSignal<K, T>>,
  reactor: AtomicTake<RequestReactor<K, T>>,
}

impl<K, T> DataLoader<K, T>
where
  K: Key,
  T: TaskHandler<K>,
{
  pub fn new() -> Self {
    let (tx, rx) = flume::unbounded::<ReactorSignal<K, T>>();
    let reactor = AtomicTake::new(RequestReactor::new(rx));
    Self { tx, reactor }
  }

  // Start processing incoming load requests in the background
  pub fn start_detached_reactor(&self) {
    if let Some(reactor) = self.reactor.take() {
      reactor.start_detached();
    }
  }

  pub fn load_by(&self, key: K) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    K: Key,
    T: TaskHandler<K>,
  {
    let (req, rx) = Request::new(key);

    self.tx.send(ReactorSignal::Load(req)).ok();

    rx
  }
}

pub trait StaticLoaderExt<K: Key, T: TaskHandler<K>> {
  fn loader() -> &'static DataLoader<K, T>;
}
