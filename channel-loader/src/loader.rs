use crate::deferral_token::DeferralToken;
use crate::reactor::{ReactorSignal, RequestReactor};
use crate::request::Request;
use crate::Key;
use atomic_take::AtomicTake;
use flume::{self, Sender};
use std::any::TypeId;
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
    deferral_token: Option<&DeferralToken>,
  ) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    K: Key,
    T: Loader<K>,
  {
    let (req, rx) = Request::new(key);

    if let Some(token) = deferral_token {
      token.enter_deferral_span(Box::new(|deferral_map| {
        let type_id = TypeId::of::<DataLoader<K, T>>();

        let deferral_active = deferral_map
          .get(&type_id)
          .expect("Missing DeferralCoordinator for DataLoader. see attach_loader macro for usage");

        if deferral_active.compare_exchange(false, true).is_ok() {
          self.tx.send(ReactorSignal::EnterDeferralSpan).ok();
        }
      }));
    }

    self.tx.send(ReactorSignal::Load(req, timing)).ok();

    rx
  }
}

pub trait StaticLoaderExt<K: Key, T: Loader<K>> {
  fn loader() -> &'static DataLoader<K, T>;
}
