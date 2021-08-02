extern crate darling;
extern crate syn;

use darling::FromDeriveInput;
use proc_macro2::{Span, TokenStream, TokenTree};
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(data_loader))]
struct LoaderArgs {
  #[darling(default)]
  internal: bool,
  #[darling(multiple)]
  handler: Vec<syn::Path>,
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(cached_loader))]
struct CachedLoaderArgs {
  #[darling(default)]
  internal: bool,
  #[darling(multiple)]
  handler: Vec<syn::Path>,
}

#[proc_macro_derive(Loadable, attributes(data_loader, cached_loader))]
pub fn load_by(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  let loader_args: LoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse data_loader attribute");
  let cached_loader_args: CachedLoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse cached_loader attribute");

  let internal = loader_args.internal || cached_loader_args.internal;
  let crate_name = get_crate_name(internal);
  let loadable = &input.ident;

  let handler = &loader_args.handler;

  let cached_handler: Vec<TokenStream> = cached_loader_args
    .handler
    .iter()
    .map(|handler| {
      quote! {
        #crate_name::redis::CacheHandler<#handler>
      }
    })
    .collect();

  let expanded = quote! {
    #(
      #[#crate_name::async_trait::async_trait]
      impl #crate_name::loadable::LoadBy<
          #handler,
          <#handler as #crate_name::task::TaskHandler>::Key,
          <#handler as #crate_name::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#handler as #crate_name::task::TaskHandler>::Error;
        async fn load_by(key: <#handler as #crate_name::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#handler as #crate_name::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#handler as #crate_name::loader::LocalLoader<#crate_name::loader::DataStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<Cache: Send + Sync + AsRef<#crate_name::request::LoadCache<
          <#handler as #crate_name::task::TaskHandler>::Key,
          <#handler as #crate_name::task::TaskHandler>::Value,
          <#handler as #crate_name::task::TaskHandler>::Error
        >>>(
          key: <#handler as #crate_name::task::TaskHandler>::Key,
          cache: &Cache
        ) -> Result<Option<std::sync::Arc<<#handler as #crate_name::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#handler as #crate_name::loader::LocalLoader<#crate_name::loader::DataStore>>::loader().with(|loader| loader.cached_load_by(key, cache));

          rx.recv().await
        }
      }
    )
    *
    #(
      #[#crate_name::async_trait::async_trait]
      impl #crate_name::loadable::LoadBy<
          #cached_handler,
          <#cached_handler as #crate_name::task::TaskHandler>::Key,
          <#cached_handler as #crate_name::task::TaskHandler>::Value,
        > for #loadable
      {
        type Error = <#cached_handler as #crate_name::task::TaskHandler>::Error;
        async fn load_by(key: <#cached_handler as #crate_name::task::TaskHandler>::Key) -> Result<Option<std::sync::Arc<<#cached_handler as #crate_name::task::TaskHandler>::Value>>, Self::Error> {
          let rx = <#cached_handler as #crate_name::loader::LocalLoader<#crate_name::loader::CacheStore>>::loader().with(|loader| loader.load_by(key));

          rx.recv().await
        }

        async fn cached_load_by<Cache: Send + Sync + AsRef<#crate_name::request::LoadCache<
          <#cached_handler as #crate_name::task::TaskHandler>::Key,
          <#cached_handler as #crate_name::task::TaskHandler>::Value,
          <#cached_handler as #crate_name::task::TaskHandler>::Error
        >>>(
          key: <#cached_handler as #crate_name::task::TaskHandler>::Key,
          cache: &Cache
        ) -> Result<Option<std::sync::Arc<<#cached_handler as #crate_name::task::TaskHandler>::Value>>, Self::Error> {
          let rx =
            <#cached_handler as #crate_name::loader::LocalLoader<#crate_name::loader::CacheStore>>::loader().with(|loader| loader.cached_load_by(key, cache));

          rx.recv().await
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(Loader, attributes(data_loader, cached_loader))]
pub fn local_loader(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);

  let loader_args: LoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse data_loader attribute");
  let cached_loader_args: CachedLoaderArgs =
    FromDeriveInput::from_derive_input(&input).expect("can't parse cached_loader attribute");

  let internal = loader_args.internal || cached_loader_args.internal;
  let crate_name = get_crate_name(internal);

  let handler: Vec<&syn::Path> = loader_args
    .handler
    .iter()
    .chain(cached_loader_args.handler.iter())
    .collect();

  let cached_handler: Vec<TokenStream> = cached_loader_args
    .handler
    .iter()
    .map(|handler| {
      quote! {
        #crate_name::redis::CacheHandler<#handler>
      }
    })
    .collect();

  let loader = input.ident;

  let expanded = quote! {
    #(
      impl #crate_name::loader::LocalLoader<#crate_name::loader::DataStore> for #loader {
        type Handler = #handler;
        fn loader() -> &'static std::thread::LocalKey<#crate_name::loader::DataLoader<Self::Handler>> {
          #[static_init::dynamic(0)]
          static WORKER_REGISTRY: #crate_name::worker::WorkerRegistry<#handler> = #crate_name::worker::WorkerRegistry::new();

          thread_local! {
            static DATA_LOADER: #crate_name::loader::DataLoader<#handler> = #crate_name::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
          }

          &DATA_LOADER
        }
      }
    )
    *
    #(
      impl #crate_name::loader::LocalLoader<#crate_name::loader::CacheStore> for #loader {
        type Handler = #cached_handler;
        fn loader() -> &'static std::thread::LocalKey<#crate_name::loader::DataLoader<Self::Handler>> {
          #[static_init::dynamic(0)]
          static WORKER_REGISTRY: #crate_name::worker::WorkerRegistry<#cached_handler> = #crate_name::worker::WorkerRegistry::new();

          thread_local! {
            static DATA_LOADER: #crate_name::loader::DataLoader<#cached_handler> = #crate_name::loader::DataLoader::from_registry(unsafe { &WORKER_REGISTRY });
          }

          &DATA_LOADER
        }
      }
    )
    *
  };

  proc_macro::TokenStream::from(expanded)
}

fn get_crate_name(internal: bool) -> TokenStream {
  if internal {
    quote! { crate }
  } else {
    let name = match crate_name("deque-loader") {
      Ok(FoundCrate::Name(name)) => name,
      Ok(FoundCrate::Itself) | Err(_) => "deque_loader".to_string(),
    };
    TokenTree::from(Ident::new(&name, Span::call_site())).into()
  }
}
