//! ```rust
//! use async_graphql::SimpleObject;
//! use channel_loader::diesel::{DieselError, DieselLoader};
//! use db::schema::users;
//! use diesel::prelude::*;
//! use diesel_connection::PooledConnection;
//! use std::collections::HashMap;
//!
//! derive_id! {
//!   #[derive(Identifiable)]
//!   #[table_name = "users"]
//!   #[graphql(name = "UserID")]
//!   pub struct UserId( #[column_name = "id"] i32);
//! }
//!
//! #[derive(SimpleObject, Identifiable, Associations, Queryable, Debug, Clone)]
//! #[belongs_to(UserId, foreign_key = "id")]
//! #[table_name = "users"]
//! pub struct User {
//!   pub id: i32,
//!   pub name: String,
//! }
//!
//! #[derive(Default)]
//! pub struct UserLoader;
//!
//! impl DieselLoader for UserLoader {
//!   type Key = UserId;
//!   type Value = Vec<User>;
//!   fn load(
//!     conn: PooledConnection,
//!     keys: Vec<UserId>,
//!   ) -> Result<HashMap<Self::Key, Self::Value>, DieselError> {
//!     let users: Vec<User> = User::belonging_to(&keys)
//!       .select(users::all_columns)
//!       .load::<User>(&conn)?;
//!
//!     let grouped_users = users.grouped_by(&keys);
//!
//!     let mut data: HashMap<UserId, Vec<User>> = HashMap::new();
//!
//!     data.extend(keys.into_iter().zip(grouped_users.into_iter()));
//!
//!     Ok(data)
//!   }
//! }
//!
//! define_static_loader!(UserLoader);
//! attach_loader!(User, UserLoader);
//! ```

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
pub mod task;

pub use key::Key;
pub use loadable::Loadable;
