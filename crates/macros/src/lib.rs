//! `dagr-macros` — the optional, build-time-only proc-macro authoring layer
//! (ADR 082, `docs/implementation/082-task-macro-adr.md`).
//!
//! This crate exports one attribute, [`macro@task`], applied to an inherent
//! `impl` block. It expands to the exact `impl Task for Foo { … }` a task author
//! writes by hand today (`dagr_core::task::Task`), so an author can write only
//! the `run` fn and have the four C1 declarations (input type, output type,
//! execution class, work) generated.
//!
//! # It is a build-time crate — never linked into a binary
//!
//! `dagr-macros` is a `proc-macro = true` crate: its only dependencies are the
//! build-time `syn` / `quote` / `proc-macro2`, and a proc-macro runs **inside
//! the compiler** and is never linked into the shipped program. `dagr-core`
//! depends on this crate only behind its default-on `macros` feature and
//! re-exports the attribute (`#[cfg(feature = "macros")] pub use
//! dagr_macros::task;`), so `use dagr_core::task;` resolves it and
//! `--no-default-features` turns it off. The expansion references only existing
//! `dagr-core` items, so the produced program's **runtime** dependency graph is
//! byte-for-byte unchanged — dagr-core's zero-runtime-dependency guarantee (ADR
//! 081) is preserved.
//!
//! # This slice (T71)
//!
//! Zero-input (`Input = ()`) and single-input (bare `Input = T`, never `(T,)`)
//! tasks, the `AwaitBound` execution class only, an optional `ctx: &RunContext`
//! parameter, and enforcement that `run` returns `Result<T, TaskError>`.
//! Multi-arity (2..=8) tasks and the `#[task(blocking)]` / `#[task(compute)]`
//! execution-class arguments are **T72**; the quickstart rewrite and the
//! `trybuild` corpus are **T73**.

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, FnArg, GenericArgument, ImplItem, ItemImpl, Pat, PatType, PathArguments,
    ReturnType, Type,
};

/// The `#[task]` attribute — expands an inherent `impl` block into an
/// `impl Task for Self` (ADR 082).
///
/// Apply it to an inherent `impl` block that contains an `async fn run`; the
/// macro reads the `run` signature and emits the trait implementation
/// deterministically:
///
/// - **Inputs → `type Input`.** Zero dependency arguments → `()`; one dependency
///   argument `x: T` → the **bare** `T` (never `(T,)` — the arity-1 blanket
///   `Deps` impl delivers the bare value). Arguments are taken **by value**.
/// - **Output → `type Output`.** Inferred from a `-> Result<T, TaskError>`
///   return type; a `run` that does not return `Result<_, TaskError>` is rejected
///   with a `compile_error!` naming the required shape.
/// - **Execution class.** `AwaitBound`, emitted unconditionally in this slice
///   (the attribute takes no argument yet — T72 adds `#[task(blocking)]` /
///   `#[task(compute)]`).
/// - **Receiver & context.** The generated `run` always takes the trait's
///   `&mut self` receiver and the trait's `ctx` parameter; the user's
///   `ctx: &RunContext` is threaded into the body only when the user declares it,
///   and is left unused (no `unused` warning) when absent.
///
/// The attribute takes **no** argument in this slice. See the module docs for
/// the deferred scope.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    // This slice's attribute takes no argument (execution-class args are T72).
    if !attr.is_empty() {
        let msg = "#[task] takes no argument in this release; \
                   #[task(blocking)] / #[task(compute)] are a later slice (T72)";
        let err = syn::Error::new(proc_macro2::TokenStream::from(attr).span(), msg);
        return err.to_compile_error().into();
    }

    let input = parse_macro_input!(item as ItemImpl);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Build the `impl Task` from the annotated inherent `impl` block, or return a
/// spanned error that becomes a `compile_error!`.
fn expand(mut item: ItemImpl) -> syn::Result<proc_macro2::TokenStream> {
    // Reject `#[task] impl Trait for Ty` — the attribute annotates an *inherent*
    // impl (the block that holds the `run` work), not a trait impl.
    if let Some((_, path, _)) = &item.trait_ {
        return Err(syn::Error::new(
            path.span(),
            "#[task] applies to an inherent `impl` block (e.g. `impl Foo { async fn run(..) .. }`), \
             not a trait impl",
        ));
    }

    let self_ty = item.self_ty.clone();

    // Take the `run` method out of the inherent impl: its body moves into the
    // generated trait `run`, and the inherent `run` is REMOVED so it cannot
    // shadow the trait method (an inherent method is preferred over a trait one
    // by Rust's method resolution — dagx renames the user method for the same
    // reason). Any OTHER inherent items the user wrote are preserved verbatim.
    let run = take_run(&mut item)?;

    // The generated `run` always carries the trait's receiver and context; the
    // body is the user's, with the user's parameter names bound.
    let output_ty = result_ok_type(&run.sig.output)?;
    let ctx_param = ctx_param(&run.sig)?;
    let (input_ty, input_param) = input_shape(&run.sig)?;
    let body = &run.block;

    // The generated context binding: the user's name when they declared `ctx`,
    // else a throwaway `_ctx` (no `unused` warning — the trait-supplied ctx is
    // simply not used when the body does not name it).
    let ctx_binding = if let Some(name) = ctx_param {
        quote!(#name)
    } else {
        quote!(_ctx)
    };

    // Re-emit the (now `run`-stripped) inherent impl only when it still carries
    // items — an empty `impl Foo {}` would be dead noise.
    let inherent = if item.items.is_empty() {
        quote!()
    } else {
        quote!(#item)
    };

    // `async fn` in the trait impl desugars to the trait's `impl Future + Send`
    // return, exactly as the hand-written impls do. The user's body is inlined
    // under the trait signature; the user's parameter names remain in scope.
    Ok(quote! {
        #inherent

        impl ::dagr_core::task::Task for #self_ty {
            type Input = #input_ty;
            type Output = #output_ty;
            const EXECUTION_CLASS: ::dagr_core::task::ExecutionClass =
                ::dagr_core::task::ExecutionClass::AwaitBound;

            async fn run(
                &mut self,
                #ctx_binding: &::dagr_core::task::RunContext,
                #input_param,
            ) -> ::core::result::Result<Self::Output, ::dagr_core::TaskError> {
                #body
            }
        }
    })
}

/// Remove the `async fn run` from the impl block and return it (owned), leaving
/// any other inherent items behind. Errors if `run` is missing or not `async`.
fn take_run(item: &mut ItemImpl) -> syn::Result<syn::ImplItemFn> {
    let idx = item
        .items
        .iter()
        .position(|it| matches!(it, ImplItem::Fn(f) if f.sig.ident == "run"));
    let Some(idx) = idx else {
        return Err(syn::Error::new(
            item.span(),
            "#[task] requires an `async fn run` in the impl block",
        ));
    };
    let ImplItem::Fn(run) = item.items.remove(idx) else {
        unreachable!("index located an ImplItem::Fn");
    };
    if run.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            run.sig.span(),
            "#[task]'s `run` must be `async fn` (a task's work is an async fn returning \
             Result<T, TaskError>)",
        ));
    }
    Ok(run)
}

/// The declared **context** parameter's binding name, if the user declared a
/// `ctx: &RunContext` (detected by its type being a reference `&_`). Returns the
/// user's chosen identifier so the body can name it. Returns `None` when the
/// `run` declares no context parameter.
///
/// A reference-typed parameter is the context; a by-value parameter is a
/// dependency input. In this slice there is at most one of each.
fn ctx_param(sig: &syn::Signature) -> syn::Result<Option<syn::Ident>> {
    for arg in typed_args(sig) {
        if matches!(&*arg.ty, Type::Reference(_)) {
            let name = pat_ident(&arg.pat).ok_or_else(|| {
                syn::Error::new(
                    arg.pat.span(),
                    "#[task]'s `ctx` parameter must be a plain identifier (e.g. `ctx: &RunContext`)",
                )
            })?;
            return Ok(Some(name));
        }
    }
    Ok(None)
}

/// The task's **input** shape: the inferred `type Input` and the parameter the
/// generated `run` binds it to.
///
/// - Zero by-value arguments → `type Input = ()`, bound to a throwaway
///   `_input: ()` (no `unused` warning).
/// - One by-value argument `x: T` → the **bare** `type Input = T` (never
///   `(T,)`), bound to the user's own `x: T` parameter so the body names it.
///
/// More than one by-value argument is multi-arity — **T72**; it is rejected here
/// with a message pointing at that slice.
fn input_shape(
    sig: &syn::Signature,
) -> syn::Result<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
    let value_args: Vec<&PatType> = typed_args(sig)
        .filter(|arg| !matches!(&*arg.ty, Type::Reference(_)))
        .collect();

    match value_args.as_slice() {
        [] => Ok((quote!(()), quote!(_input: ()))),
        [only] => {
            let ty = &only.ty;
            let pat = &only.pat;
            // Bare `T`, never `(T,)`: the arity-1 blanket `Deps` impl delivers
            // the bare value. Bind the user's own parameter so the body names it.
            Ok((quote!(#ty), quote!(#pat: #ty)))
        }
        _ => Err(syn::Error::new(
            value_args[1].span(),
            "#[task] supports zero or one dependency input in this release; \
             multi-input (2..=8) tasks are a later slice (T72) — for now, aggregate \
             the inputs into a struct produced by an intermediate node",
        )),
    }
}

/// The `T` in a `-> Result<T, TaskError>` return type. A `run` that does not
/// return `Result<_, TaskError>` is rejected with a message naming the required
/// shape (ADR 082: the failure channel is explicit; custom error types are out
/// of scope for M5).
fn result_ok_type(output: &ReturnType) -> syn::Result<Type> {
    let shape_err = |span| {
        syn::Error::new(
            span,
            "#[task]'s `run` must return `Result<T, TaskError>` \
             (a task must be able to fail with a classified error)",
        )
    };

    let ty = match output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => return Err(shape_err(output.span())),
    };

    // Match `Result< Ok , _ >`: take the last path segment named `Result` with
    // two angle-bracketed type arguments. We do not attempt to prove the second
    // arg is literally `TaskError` (a path/alias would defeat that), but the
    // generated impl binds the trait's `Result<Self::Output, TaskError>`, so a
    // divergent error type surfaces as a natural type error at the trait
    // signature — the required shape is still named here.
    let Type::Path(type_path) = &**ty else {
        return Err(shape_err(ty.span()));
    };
    let seg = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| shape_err(ty.span()))?;
    if seg.ident != "Result" {
        return Err(shape_err(ty.span()));
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return Err(shape_err(ty.span()));
    };
    let mut types = args.args.iter().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t.clone()),
        _ => None,
    });
    let ok = types.next().ok_or_else(|| shape_err(ty.span()))?;
    // `Result<T, E>` has exactly two type arguments; a one-arg `Result<T>` alias
    // is not the shape a task returns.
    if types.next().is_none() {
        return Err(shape_err(ty.span()));
    }
    Ok(ok)
}

/// The typed (non-receiver) arguments of a signature, in order.
fn typed_args(sig: &syn::Signature) -> impl Iterator<Item = &PatType> {
    sig.inputs.iter().filter_map(|arg| match arg {
        FnArg::Typed(pt) => Some(pt),
        FnArg::Receiver(_) => None,
    })
}

/// The identifier of a plain-identifier pattern, if the pattern is one.
fn pat_ident(pat: &Pat) -> Option<syn::Ident> {
    match pat {
        Pat::Ident(pi) => Some(pi.ident.clone()),
        _ => None,
    }
}
