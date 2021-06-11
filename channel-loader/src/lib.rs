#![allow(dead_code)]
#[allow(rustdoc::private_intra_doc_links)]
#[doc(hidden)]
pub extern crate atomic_take;
#[doc(hidden)]
pub extern crate booter;
#[doc(hidden)]
pub extern crate crossbeam;
#[doc(hidden)]
pub extern crate static_init;

#[cfg(feature = "diesel-loader")]
pub mod diesel;
mod key;
#[doc(hidden)]
pub mod loadable;
#[doc(hidden)]
pub mod loader;
mod reactor;
mod request;
mod simple_loader;
pub mod task;

pub use key::Key;
pub use loadable::Loadable;
pub use simple_loader::SimpleLoader;
