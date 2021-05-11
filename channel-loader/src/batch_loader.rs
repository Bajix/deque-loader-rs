use crate::key::Key;
use crate::loader::{DataLoader, Loader};
use crate::reactor::ReactorSignal;
pub struct DeferralGuard {
  drop_fn: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl DeferralGuard {
  pub(crate) fn new(drop_fn: Box<dyn FnOnce() + Send + Sync>) -> Self {
    DeferralGuard {
      drop_fn: Some(drop_fn),
    }
  }
}

impl Drop for DeferralGuard {
  fn drop(&mut self) {
    if let Some(drop_fn) = self.drop_fn.take() {
      drop_fn();
    }
  }
}

pub trait BatchingExt<K: Key, T: Loader<K>> {
  fn get_batching_guard(&self) -> DeferralGuard;
}

impl<K, T> BatchingExt<K, T> for DataLoader<K, T>
where
  K: Key,
  T: Loader<K>,
{
  fn get_batching_guard(&self) -> DeferralGuard {
    self.tx.send(ReactorSignal::EnterDeferralSpan).ok();
    let tx = self.tx.clone();

    DeferralGuard::new(Box::new(move || {
      tx.send(ReactorSignal::ExitDeferralSpan).ok();
    }))
  }
}
