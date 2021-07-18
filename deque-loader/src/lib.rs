//! ```rust
//! use async_graphql::SimpleObject;
//! use deque_loader::diesel::{DieselError, DieselLoader};
//! use db::schema::users;
//! use diesel::prelude::*;
//! use diesel_connection::PooledConnection;
//! use std::{collections::HashMap, sync::Arc};
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
//! pub struct UserLoader;
//!
//! impl DieselLoader for UserLoader {
//!   type Key = UserId;
//!   type Value = User;
//!   fn load(
//!     conn: PooledConnection,
//!     keys: Vec<UserId>,
//!   ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError> {
//!     let users: Vec<User> = User::belonging_to(&keys)
//!       .select(users::all_columns)
//!       .load::<User>(&conn)?;
//!
//!     let users = users.into_iter().map(Arc::new);
//!
//!     let mut data: HashMap<UserId, Arc<User>> = HashMap::new();
//!
//!     data.extend(keys.into_iter().zip(users));
//!
//!     Ok(data)
//!   }
//! }
//!
//! define_diesel_loader!(UserLoader);
//! attach_loader!(User, UserLoader);
//! ```

#![allow(dead_code)]
#[doc(hidden)]
#[allow(rustdoc::private_intra_doc_links)]
pub extern crate async_trait;
#[doc(hidden)]
#[cfg(feature = "diesel-loader")]
pub extern crate diesel_connection;
#[doc(hidden)]
pub extern crate static_init;
pub mod batch;
#[cfg(feature = "diesel-loader")]
pub mod diesel;
#[cfg(feature = "graphql")]
pub mod graphql;
mod key;
#[doc(hidden)]
pub mod loadable;
pub mod loader;
#[doc(hidden)]
pub mod request;
pub mod task;
#[doc(hidden)]
pub mod worker;

pub use key::Key;
pub use loadable::Loadable;
