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
//! # What this crate expands (T71 + T72)
//!
//! - **Inputs → `type Input`.** Zero dependency inputs → `()`; one → the **bare**
//!   value `T` (never `(T,)`); **2..=8** → the tuple `(A, B, …)`. A multi-input
//!   task is written with **one non-destructured tuple parameter**
//!   (`input: (A, B)`), or equivalently a destructuring tuple pattern
//!   (`(a, b): (A, B)`) so the body observes the named bindings; either way the
//!   macro binds that single by-value parameter directly and `type Input` is its
//!   tuple type. Arguments are taken **by value** (dagr delivers inputs by value;
//!   receive mode lives at the registration site, not the task body).
//! - **Execution class → the impl-level `EXECUTION_CLASS` const.** Taken from the
//!   **attribute argument**, never inferred from the body: `#[task]` and
//!   `#[task()]` → `AwaitBound`; `#[task(blocking)]` → `Blocking`;
//!   `#[task(compute)]` → `Compute`.
//! - **Optional `ctx: &RunContext`** (detected by type) and the
//!   `Result<T, TaskError>` return requirement (a bare `-> T` is a
//!   `compile_error!`) — unchanged from T71.
//!
//! The **input-arity ceiling is 8**: a single tuple parameter of more than 8
//! elements is not rejected by the macro — the tuple type flows through as
//! `type Input`, and it is the sealed `dagr_core::binding::Deps` trait (whose
//! tuple impls stop at `MAX_INPUT_ARITY = 8`) that surfaces the curated
//! `#[diagnostic::on_unimplemented]` "too many inputs" error **at the
//! registration site** when such a task is wired. The macro adds no second
//! ceiling check; the one authoritative cliff is the `Deps` one. (Writing more
//! than 8 **separate** by-value parameters is a different misuse — the surface is
//! a single tuple parameter — and is rejected here with a message pointing at the
//! tuple form.)
//!
//! (These `dagr_core` paths are written as plain code, not intra-doc links: this
//! is a build-time proc-macro crate that does not depend on `dagr_core`, so its
//! rustdoc cannot resolve links into it.)
//!
//! The quickstart rewrite and the `trybuild` corpus are **T73**.

use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, FnArg, GenericArgument, ImplItem, ItemImpl, Pat, PatType, PathArguments,
    ReturnType, Type,
};

/// The execution class the attribute argument selected, mapped to the
/// `dagr_core::task::ExecutionClass` variant the generated impl emits.
///
/// The class is taken **only** from the attribute — `#[task]`/`#[task()]` →
/// `AwaitBound`, `#[task(blocking)]` → `Blocking`, `#[task(compute)]` → `Compute`
/// — and is never inferred from the `run` body.
#[derive(Clone, Copy)]
enum ExecClass {
    AwaitBound,
    Blocking,
    Compute,
}

impl ExecClass {
    /// The `ExecutionClass` variant path this class emits as the impl-level const.
    fn variant(self) -> proc_macro2::TokenStream {
        match self {
            ExecClass::AwaitBound => quote!(::dagr_core::task::ExecutionClass::AwaitBound),
            ExecClass::Blocking => quote!(::dagr_core::task::ExecutionClass::Blocking),
            ExecClass::Compute => quote!(::dagr_core::task::ExecutionClass::Compute),
        }
    }
}

/// The `#[task]` attribute — expands an inherent `impl` block into an
/// `impl Task for Self` (ADR 082).
///
/// Apply it to an inherent `impl` block that contains an `async fn run`; the
/// macro reads the `run` signature and emits the trait implementation
/// deterministically:
///
/// - **Inputs → `type Input`.** Zero dependency arguments → `()`; one dependency
///   argument `x: T` → the **bare** `T` (never `(T,)` — the arity-1 blanket
///   `Deps` impl delivers the bare value); a single **tuple** parameter of 2..=8
///   elements (`input: (A, B)` or the destructuring `(a, b): (A, B)`) → the tuple
///   `type Input = (A, B, …)`. Arguments are taken **by value**.
/// - **Output → `type Output`.** Inferred from a `-> Result<T, TaskError>`
///   return type; a `run` that does not return `Result<_, TaskError>` is rejected
///   with a `compile_error!` naming the required shape.
/// - **Execution class.** Taken from the **attribute argument**, emitted as the
///   impl-level associated const, never inferred from the body: `#[task]` and
///   `#[task()]` → `AwaitBound`, `#[task(blocking)]` → `Blocking`,
///   `#[task(compute)]` → `Compute`.
/// - **Receiver & context.** The generated `run` always takes the trait's
///   `&mut self` receiver and the trait's `ctx` parameter; the user's
///   `ctx: &RunContext` is threaded into the body only when the user declares it,
///   and is left unused (no `unused` warning) when absent.
///
/// The attribute argument grammar is exactly `blocking` / `compute` / empty; any
/// other argument is a `compile_error!`.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let class = match parse_exec_class(attr) {
        Ok(class) => class,
        Err(err) => return err.to_compile_error().into(),
    };
    let input = parse_macro_input!(item as ItemImpl);
    expand(input, class)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Parse the attribute argument into an [`ExecClass`]: empty (`#[task]` /
/// `#[task()]`) → `AwaitBound`, the bare identifier `blocking` → `Blocking`, the
/// bare identifier `compute` → `Compute`. Any other token is a spanned error
/// naming the accepted grammar (ADR 082: the grammar is exactly
/// `blocking`/`compute`/empty; other opt-in markers are out of scope).
fn parse_exec_class(attr: TokenStream) -> syn::Result<ExecClass> {
    let attr = proc_macro2::TokenStream::from(attr);
    if attr.is_empty() {
        return Ok(ExecClass::AwaitBound);
    }
    // The argument is a single bare identifier — parse it as an `Ident` so a
    // stray path, literal, or extra token is rejected with a clear message.
    let ident: syn::Ident = syn::parse2(attr.clone()).map_err(|_| {
        syn::Error::new(
            attr.span(),
            "#[task] accepts at most one execution-class argument: \
             `blocking`, `compute`, or none (`#[task]`)",
        )
    })?;
    match ident.to_string().as_str() {
        "blocking" => Ok(ExecClass::Blocking),
        "compute" => Ok(ExecClass::Compute),
        other => Err(syn::Error::new(
            ident.span(),
            format!(
                "unknown #[task] execution class `{other}`: the accepted arguments are \
                 `blocking`, `compute`, or none (`#[task]` = await-bound)"
            ),
        )),
    }
}

/// Build the `impl Task` from the annotated inherent `impl` block, or return a
/// spanned error that becomes a `compile_error!`. `class` is the execution class
/// the attribute selected (emitted verbatim as the impl-level const).
fn expand(mut item: ItemImpl, class: ExecClass) -> syn::Result<proc_macro2::TokenStream> {
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

    // The execution class comes from the attribute, emitted verbatim as the
    // impl-level const — never inferred from the body (ADR 082).
    let exec_class = class.variant();

    // `async fn` in the trait impl desugars to the trait's `impl Future + Send`
    // return, exactly as the hand-written impls do. The user's body is inlined
    // under the trait signature; the user's parameter names remain in scope.
    Ok(quote! {
        #inherent

        impl ::dagr_core::task::Task for #self_ty {
            type Input = #input_ty;
            type Output = #output_ty;
            const EXECUTION_CLASS: ::dagr_core::task::ExecutionClass = #exec_class;

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
/// dagr delivers a task's input **as a single by-value value** — the bare `T` for
/// one input, the tuple `(A, B, …)` for many (`binding.rs`) — so the authoring
/// surface is a **single** by-value `run` parameter, and its type *is* `Input`:
///
/// - Zero by-value parameters → `type Input = ()`, bound to a throwaway
///   `_input: ()` (no `unused` warning).
/// - One by-value parameter `p: T` → `type Input = T`, bound to the user's own
///   pattern `p` so the body names it. This covers **both** the single bare input
///   (`x: u64` → `type Input = u64`, never `(u64,)`) and the multi-input tuple
///   (`input: (A, B)` or the destructuring `(a, b): (A, B)` → `type Input =
///   (A, B, …)`): binding the tuple pattern directly *is* the `let (a, b) = input;`
///   the multi-input author gets, so a destructuring parameter observes its named
///   bindings in declared order.
///
/// Two or more **separate** by-value parameters is a misuse — the surface is a
/// single tuple parameter — and is rejected here with a message pointing at that
/// form. The 8-input ceiling is **not** enforced here: an over-8 tuple flows
/// through as `type Input` and the sealed `Deps` trait raises the curated "too
/// many inputs" diagnostic at the registration site (see the module docs).
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
            // The single by-value parameter's type IS `Input` (bare `T`, or the
            // tuple `(A, B, …)` for a multi-input task). Bind the user's own
            // pattern so the body names it — for a destructuring tuple pattern
            // that is exactly the `let (a, b) = input;` a multi-input task needs.
            Ok((quote!(#ty), quote!(#pat: #ty)))
        }
        _ => Err(syn::Error::new(
            value_args[1].span(),
            "#[task] takes a single by-value input parameter; a multi-input task \
             binds one tuple parameter (e.g. `input: (A, B)` or `(a, b): (A, B)`), \
             not several separate parameters",
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
