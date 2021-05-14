use crate::deferral_token::DeferralToken;
use crate::key::Key;
use crate::loader::{LoadTiming, Loader};
use crate::reactor::ReactorSignal;
use crossbeam::atomic::AtomicCell;
use tokio::sync::oneshot;

pub trait Loadable<K: Key, T: Loader<K>> {
  fn load_by(
    key: K,
    timing: LoadTiming,
    deferral_token: Option<&DeferralToken>,
  ) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    K: Key,
    T: Loader<K>;
}

#[macro_export]
macro_rules! attach_loader {
  ($loadable:ty, $loader:ty, $key:ty) => {
    use std::any::TypeId;

    use $crate::{
      booter,
      deferral_token::{DeferralCoordinator, DeferralToken},
      loadable::Loadable,
      loader::{DataLoader, StaticLoaderExt},
      static_init,
    };

    #[static_init::dynamic(0)]
    static mut LOADER: DataLoader<$key, $loader> = DataLoader::new(<$loader>::default());

    impl StaticLoaderExt<$key, $loader> for DataLoader<$key, $loader> {
      fn loader() -> &'static DataLoader<$key, $loader> {
        unsafe { &LOADER }
      }
    }

    impl Loadable<$key, $loader> for $loadable {
      fn load_by(
        key: $key,
        timing: LoadTiming,
        deferral_token: Option<&DeferralToken>,
      ) -> oneshot::Receiver<
        Result<Option<<$loader as Loader<$key>>::Value>, <$loader as Loader<$key>>::Error>,
      > {
        <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader().load_by(
          key,
          timing,
          deferral_token,
        )
      }
    }

    inventory::submit!({
      DeferralCoordinator::new(
        Box::new(|guards| {
          let type_id = TypeId::of::<DataLoader<$key, $loader>>();
          let deferral_active = AtomicCell::<bool>::new(false);
          guards.insert(type_id, deferral_active);
        }),
        Box::new(|deferral_map| {
          let type_id = TypeId::of::<DataLoader<$key, $loader>>();

          let deferral_active = deferral_map.get(&type_id).expect(
            "Missing DeferralCoordinator for DataLoader. see attach_loader macro for usage",
          );

          if deferral_active.compare_exchange(true, false).is_ok() {
            <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader()
              .tx
              .send(ReactorSignal::ExitDeferralSpan)
              .ok();
          }
        }),
      )
    });

    booter::call_on_boot!({
      <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader()
        .start_detached_reactor();
    });
  };
}

mod tests {
  use super::*;
  use crate::loader::LoadTiming;
  use std::collections::HashMap;
  use std::iter;

  #[derive(Default)]
  pub struct LoremLoader {}

  #[derive(Clone)]
  pub struct Ipsum {
    pub content: String,
  }

  impl Default for Ipsum {
    fn default() -> Self {
      Ipsum {
        content: String::from("Lorem ipsum dolor sit amet"),
      }
    }
  }

  #[async_trait::async_trait]
  impl Loader<i32> for LoremLoader {
    type Value = Ipsum;
    type Error = ();

    async fn load(&self, ids: Vec<i32>) -> Result<HashMap<i32, Self::Value>, Self::Error> {
      let mut data: HashMap<i32, Ipsum> = HashMap::new();

      data.extend(ids.into_iter().zip(iter::repeat(Ipsum::default())));

      Ok(data)
    }
  }

  attach_loader!(Ipsum, LoremLoader, i32);

  #[tokio::test]
  async fn it_loads() -> Result<(), ()> {
    booter::boot();

    let data = Ipsum::load_by(10_i32, LoadTiming::Immediate, None)
      .await
      .unwrap()?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_deadline_loads() -> Result<(), ()> {
    booter::boot();

    let data = Ipsum::load_by(10_i32, LoadTiming::Deadline, None)
      .await
      .unwrap()?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_defers_loading() -> Result<(), ()> {
    booter::boot();

    let deferral_token = DeferralToken::default();

    let receiver = Ipsum::load_by(10_i32, LoadTiming::Immediate, Some(&deferral_token));

    deferral_token.send_pending_requests();

    let data = receiver.await.unwrap()?;

    assert!(data.is_some());

    Ok(())
  }
}
