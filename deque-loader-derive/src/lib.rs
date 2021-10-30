extern crate darling;
extern crate syn;

use darling::FromMeta;
use proc_macro2::TokenStream;
use quote::quote;
use std::vec;
use syn::{parse_macro_input, Attribute, DeriveInput};

#[derive(FromMeta)]
struct DataLoaderAttr {
  handler: syn::Path,
  #[darling(default)]
  cached: bool,
}

fn parse_data_loaders(attrs: &[Attribute]) -> darling::Result<Vec<DataLoaderAttr>> {
  let loader_iter = attrs
    .iter()
    .filter(|attr| attr.path.is_ident("data_loader"))
    .map(|attr| {
      let meta = darling::util::parse_attribute_to_meta_list(attr)?;
      let items: Vec<syn::NestedMeta> = meta.nested.into_iter().collect();
      let loader = DataLoaderAttr::from_list(items.as_slice())?;

      Ok::<_, darling::Error>(loader)
    });

  let mut loaders: Vec<DataLoaderAttr> = vec![];
  let mut errors: Vec<darling::Error> = vec![];

  for result in loader_iter {
    match result {
      Ok(loader) => loaders.push(loader),
      Err(err) => errors.push(err),
    };
  }

  if errors.is_empty() {
    Ok(loaders)
  } else {
    Err(darling::Error::multiple(errors))
  }
}

#[proc_macro_derive(Loadable, attributes(data_loader))]
pub fn load_by(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  let loadable = &input.ident;

  let loaders = match parse_data_loaders(&input.attrs) {
    Ok(loaders) => loaders,
    Err(err) => return proc_macro::TokenStream::from(err.write_errors()),
  };

  let handler: Vec<&syn::Path> = loaders
    .iter()
    .filter_map(|loader| {
      if loader.cached {
        None
      } else {
        Some(&loader.handler)
      }
    })
    .collect();

  let cached_handler: Vec<TokenStream> = loaders
    .iter()
    .filter_map(|loader| {
      let DataLoaderAttr { handler, cached } = loader;
      if *cached {
        Some(quote! {
          deque_loader::redis::RedisCacheAdapter<#handler>
        })
      } else {
        None
      }
    })
    .collect();

  let expanded = quote! {
    #(
      #[deque_loader::async_trait::async_trait]
      impl deque_loader::loadable::LoadBy<
          #handler,
          <#handler as deque_loader::task::TaskHandler>::Key,
          <#handler as deque_loader::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#handler as deque_loader::task::TaskHandler>::Error;
        async fn load_by(key: <#handler as deque_loader::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#handler as deque_loader::loader::LocalLoader<deque_loader::loader::DataStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<RequestCache: Send + Sync + AsRef<deque_loader::request::ContextCache<#handler>>>(
          key: <#handler as deque_loader::task::TaskHandler>::Key,
          request_cache: &RequestCache
        ) -> Result<Option<std::sync::Arc<<#handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#handler as deque_loader::loader::LocalLoader<deque_loader::loader::DataStore>>::loader().with(|loader| loader.cached_load_by(key, request_cache));

          rx.recv().await
        }
      }
    )
    *
    #(
      #[deque_loader::async_trait::async_trait]
      impl deque_loader::loadable::LoadBy<
          #cached_handler,
          <#cached_handler as deque_loader::task::TaskHandler>::Key,
          <#cached_handler as deque_loader::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#cached_handler as deque_loader::task::TaskHandler>::Error;
        async fn load_by(key: <#cached_handler as deque_loader::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#cached_handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#cached_handler as deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<RequestCache: Send + Sync + AsRef<deque_loader::request::ContextCache<#cached_handler>>>(
          key: <#cached_handler as deque_loader::task::TaskHandler>::Key,
          request_cache: &RequestCache
        ) -> Result<Option<std::sync::Arc<<#cached_handler as deque_loader::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#cached_handler as deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore>>::loader().with(|loader| loader.cached_load_by(key, request_cache));

          rx.recv().await
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(Loader, attributes(data_loader))]
pub fn local_loader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  let loader = input.ident;

  let loaders = match parse_data_loaders(&input.attrs) {
    Ok(loaders) => loaders,
    Err(err) => return proc_macro::TokenStream::from(err.write_errors()),
  };

  let handler: Vec<&syn::Path> = loaders.iter().map(|loader| &loader.handler).collect();

  let cached_handler: Vec<TokenStream> = loaders
    .iter()
    .filter_map(|loader| {
      let DataLoaderAttr { handler, cached } = loader;
      if *cached {
        Some(quote! {
          deque_loader::redis::RedisCacheAdapter<#handler>
        })
      } else {
        None
      }
    })
    .collect();

  let expanded = quote! {
    #(
      impl deque_loader::loader::LocalLoader<deque_loader::loader::DataStore> for #loader {
        type Handler = #handler;
        fn loader() -> &'static std::thread::LocalKey<deque_loader::loader::DataLoader<Self::Handler>> {
          thread_local! {
            static DATA_LOADER: deque_loader::loader::DataLoader<#handler> = deque_loader::loader::DataLoader::default();
          }

          &DATA_LOADER
        }
      }
    )
    *
    #(
      impl deque_loader::loader::LocalLoader<deque_loader::loader::CacheStore> for #loader {
        type Handler = #cached_handler;
        fn loader() -> &'static std::thread::LocalKey<deque_loader::loader::DataLoader<Self::Handler>> {
          thread_local! {
            static DATA_LOADER: deque_loader::loader::DataLoader<#cached_handler> = deque_loader::loader::DataLoader::default();
          }

          &DATA_LOADER
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}
