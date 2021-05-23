use crossbeam::atomic::AtomicCell;
use fnv::FnvHashMap;
use std::any::TypeId;

#[doc(hidden)]
pub type DeferralMap = FnvHashMap<TypeId, AtomicCell<bool>>;

#[doc(hidden)]
pub struct DeferralCoordinator {
  build_fn: Box<dyn Fn(&mut DeferralMap)>,
  reset_fn: Box<dyn Fn(&DeferralMap)>,
}

impl DeferralCoordinator {
  pub fn new(build_fn: Box<dyn Fn(&mut DeferralMap)>, reset_fn: Box<dyn Fn(&DeferralMap)>) -> Self {
    DeferralCoordinator { build_fn, reset_fn }
  }
}

inventory::collect!(DeferralCoordinator);
pub struct DeferralToken(DeferralMap);

impl DeferralToken {
  pub(crate) fn enter_deferral_span<'a>(&'a self, enter_fn: Box<dyn 'a + FnOnce(&'a DeferralMap)>) {
    (enter_fn)(&self.0);
  }

  pub fn send_pending_requests(&self) {
    for coordinator in inventory::iter::<DeferralCoordinator> {
      (coordinator.reset_fn)(&self.0);
    }
  }
}

impl Default for DeferralToken {
  fn default() -> Self {
    let mut deferral_map: DeferralMap = FnvHashMap::default();

    for coordinator in inventory::iter::<DeferralCoordinator> {
      (coordinator.build_fn)(&mut deferral_map);
    }

    DeferralToken(deferral_map)
  }
}

impl Drop for DeferralToken {
  fn drop(&mut self) {
    self.send_pending_requests();
  }
}
