use crate::reactor::{ReactorSignal, RequestReactor};
use crate::request::Request;
use crate::Key;
use atomic_take::AtomicTake;
use flume::{self, Sender};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

#[async_trait::async_trait]
pub trait Loader<K: Key>: Default + Send + Sync + 'static {
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_DELAY: Duration = Duration::from_millis(3);
  const MAX_BATCH_SIZE: usize = 100;
  async fn load(&self, keys: Vec<K>) -> Result<HashMap<K, Self::Value>, Self::Error>;
}

pub enum LoadTiming {
  Immediate,
  Deadline,
}
pub struct DataLoader<K: Key, T: Loader<K> + 'static> {
  pub(crate) tx: Sender<ReactorSignal<K, T>>,
  reactor: AtomicTake<RequestReactor<K, T>>,
}

impl<K, T> DataLoader<K, T>
where
  K: Key,
  T: Loader<K>,
{
  pub fn new(loader: T) -> Self {
    let (tx, rx) = flume::unbounded::<ReactorSignal<K, T>>();
    let reactor = AtomicTake::new(RequestReactor::new(rx, loader));
    Self { tx, reactor }
  }

  // Start processing incoming load requests in the background
  pub fn start_detached_reactor(&self) {
    if let Some(reactor) = self.reactor.take() {
      reactor.start_detached();
    }
  }

  pub fn load_by(
    &self,
    key: K,
    timing: LoadTiming,
  ) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    K: Key,
    T: Loader<K>,
  {
    let (req, rx) = Request::new(key);

    let signal = ReactorSignal::Load(req, timing);

    self.tx.send(signal).ok();

    rx
  }
}

pub trait StaticLoaderExt<K: Key, T: Loader<K>> {
  fn loader() -> &'static DataLoader<K, T>;
}
