pub use async_graphql::{EmptyMutation, EmptySubscription};

mod mutation_root;
mod query_root;

pub use mutation_root::MutationRoot;
pub use query_root::QueryRoot;

pub type Schema<QueryRoot, MutationRoot, EmptySubscription> =
  async_graphql::Schema<QueryRoot, MutationRoot, EmptySubscription>;
