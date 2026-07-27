//! `dagr-macros` — the optional, build-time-only proc-macro authoring layer.
//!
//! This crate exports two attributes and a derive:
//!
//! - [`macro@task`], applied to an inherent `impl` block, expands to the exact
//!   `impl Task for Foo { … }` a task author writes by hand today
//!   (`dagr_core::task::Task`), so an author can write only the `run` fn and have
//!   the four declarations (input type, output type, execution class, work)
//!   generated.
//! - [`macro@dag`], applied to a `fn(&mut FlowBuilder)`, keeps the fn and emits a
//!   DAG factory plus its `inventory` registration for auto-discovery (ADR 092).
//! - [`derive@StableName`], derived on a task or payload **struct**, emits the
//!   one-line `impl StableName` (`STABLE_NAME = "<ident>"`, overridable with
//!   `#[stable_name = "…"]`) that the graph-emittable registrars require — so a
//!   `#[task]` task and its payloads compose with `#[dag]` / `FlowBuilder::source`
//!   / `node` with no hand-written trait bodies.
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
//! byte-for-byte unchanged — dagr-core's zero-runtime-dependency guarantee is
//! preserved.
//!
//! # What this crate expands
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
//!   `compile_error!`).
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

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, DeriveInput, Expr, ExprLit, FnArg, GenericArgument, ImplItem, ItemFn,
    ItemImpl, Lit, Meta, Pat, PatType, PathArguments, ReturnType, Type,
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
/// `impl Task for Self`.
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
/// naming the accepted grammar (the grammar is exactly
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
    // by Rust's method resolution). Any OTHER inherent items the user wrote are
    // preserved verbatim.
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
    // impl-level const — never inferred from the body.
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
/// shape (the failure channel is explicit; custom error types are out of scope).
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

/// The `#[dag]` attribute — declares a DAG over the `FlowBuilder` façade and
/// auto-registers it for discovery.
///
/// Apply it to a flow-builder fn `fn NAME(f: &mut FlowBuilder) { … }` (the body
/// declares nodes through the `FlowBuilder` façade). The macro is a sibling to
/// [`macro@task`] and follows its discipline exactly — build-time only, zero runtime
/// dependency, expansion referencing only existing items in the caller's dependency
/// graph — but where `#[task]` expands to `::dagr_core::…`, `#[dag]` expands to
/// `::dagr_cli::…` **and** `::inventory::…`, so the app crate depends on both
/// `dagr-cli` and `inventory` (ADR 092; the `$crate`-based `inventory::submit!` offers
/// no path override).
///
/// It **keeps the user's fn verbatim** and emits two **sibling items** at module
/// (item) scope — never inside the fn body:
///
/// - a factory `fn __dag_factory_<fn>() -> ::dagr_cli::run_flow::RunnableFlow` that
///   news a [`RunnableFlow`](../dagr_cli/run_flow/struct.RunnableFlow.html), wraps a
///   `&mut` of it in a [`FlowBuilder`](../dagr_cli/flow_builder/struct.FlowBuilder.html),
///   calls the user's fn, and returns the flow (a fresh flow per verb — the flow is
///   consumed by a run, so it cannot be reused);
/// - an `::inventory::submit!{ ::dagr_cli::DagRegistration { name, factory } }`, which
///   `dagr_cli::run` collects at link time to build its `FlowRegistry`.
///
/// The factory's item name is derived from the fn ident, so multiple `#[dag]`s in one
/// module never clash at the Rust-item level. (Duplicate *DAG names* are a runtime
/// concern `dagr_cli::run` already rejects.)
///
/// # Attribute grammar
///
/// - `#[dag]` — the DAG name is the **fn name**.
/// - `#[dag(name = "…")]` — the DAG name is the given string literal.
/// - Any other argument (`#[dag(bogus)]`, `#[dag(name = 42)]`, …) is a spanned
///   `compile_error!` naming the accepted `name = "…"` grammar, mirroring
///   `#[task]`'s attribute rejection.
///
/// (The `::dagr_cli` / `::inventory` paths above are written as plain code, not
/// intra-doc links: this build-time proc-macro crate does not depend on either at
/// build time, so its rustdoc cannot resolve links into them.)
#[proc_macro_attribute]
pub fn dag(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let name = match parse_dag_name(attr, &func) {
        Ok(name) => name,
        Err(err) => return err.to_compile_error().into(),
    };
    expand_dag(&func, &name).into()
}

/// The DAG-name string literal `#[dag]` registers with: empty attribute → the fn
/// name; `name = "…"` → the given literal. Any other token is a spanned error naming
/// the accepted grammar (mirrors [`parse_exec_class`]).
fn parse_dag_name(attr: TokenStream, func: &ItemFn) -> syn::Result<syn::LitStr> {
    let attr = proc_macro2::TokenStream::from(attr);
    let grammar = "#[dag] accepts an optional `name = \"…\"` argument (a string literal) \
                   or none (`#[dag]` = the fn name)";

    // Empty attribute: the DAG name defaults to the fn's own identifier.
    if attr.is_empty() {
        let ident = &func.sig.ident;
        return Ok(syn::LitStr::new(&ident.to_string(), ident.span()));
    }

    // Otherwise the sole accepted form is the `name = "…"` name-value pair; parse it
    // as a `Meta` so a bare identifier / wrong key / non-string value each map to a
    // clear, spanned message.
    let meta: Meta =
        syn::parse2(attr.clone()).map_err(|_| syn::Error::new(attr.span(), grammar))?;
    let Meta::NameValue(nv) = &meta else {
        // `#[dag(bogus)]` / `#[dag(name(…))]` — a path or list, not `key = value`.
        return Err(syn::Error::new(meta.span(), grammar));
    };
    if !nv.path.is_ident("name") {
        // The right shape but the wrong key (`#[dag(alias = "…")]`).
        return Err(syn::Error::new(nv.path.span(), grammar));
    }
    // The value must be a string literal (`#[dag(name = 42)]` is rejected here).
    match &nv.value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s.clone()),
        other => Err(syn::Error::new(other.span(), grammar)),
    }
}

/// Keep the user's flow-builder `func` verbatim and emit, at item scope, the
/// discovery-registered factory + the `inventory::submit!` of a `DagRegistration`
/// under `name`. The factory's item ident is derived from the fn ident so multiple
/// `#[dag]`s in one module do not collide. This step is infallible — every grammar
/// error is raised in [`parse_dag_name`] before it runs.
fn expand_dag(func: &ItemFn, name: &syn::LitStr) -> proc_macro2::TokenStream {
    let fn_ident = &func.sig.ident;
    // Derive a collision-free factory item name from the fn ident: two `#[dag]`s in
    // one module have distinct fn idents, so their factories have distinct idents.
    let factory = format_ident!("__dag_factory_{}", fn_ident);

    // The factory news a fresh `RunnableFlow`, hands the user's fn a `FlowBuilder`
    // borrowing it, then returns the flow — the every-verb-its-own-fresh-flow shape
    // `DagRegistration` requires (the flow is consumed by a run, not `Clone`).
    quote! {
        #func

        #[doc(hidden)]
        fn #factory() -> ::dagr_cli::run_flow::RunnableFlow {
            let mut flow = ::dagr_cli::run_flow::RunnableFlow::new();
            {
                let mut builder = ::dagr_cli::flow_builder::FlowBuilder::new(&mut flow);
                #fn_ident(&mut builder);
            }
            flow
        }

        ::inventory::submit! {
            ::dagr_cli::DagRegistration {
                name: #name,
                factory: #factory,
            }
        }
    }
}

/// Derive [`StableName`](dagr_core::stable_name::StableName) for a task or payload
/// struct — the one-line ergonomic the trait's own docs anticipated.
///
/// The graph-emittable registrars (`FlowBuilder::source` / `node`,
/// `RunnableFlow::register_source_named` / `register_named`) require every task and
/// payload type to carry a `StableName`. `#[task]` deliberately does **not** emit
/// one (it annotates the `impl` block, not the struct; auto-emitting would collide
/// with an author who wants a custom name). This derive is the counterpart: put it
/// on the struct, and the two macros compose with no hand-written `impl StableName`.
///
/// # Grammar
///
/// - `#[derive(StableName)]` — the stable name is the **struct's own identifier**,
///   as a source-level string literal (`STABLE_NAME = "Foo"`).
/// - `#[derive(StableName)] #[stable_name = "warehouse.Rows"]` — the stable name is
///   the given string literal, so a later *Rust* rename does not move the recorded
///   identity.
/// - Any other form of the helper attribute (`#[stable_name]` with no value,
///   `#[stable_name = 42]`, `#[stable_name("x")]`, or two `#[stable_name = …]`) is a
///   spanned `compile_error!` naming the accepted `#[stable_name = "…"]` shape,
///   mirroring `#[dag]`'s attribute rejection.
///
/// # Generics
///
/// Generic structs are supported: the impl reproduces the struct's own generics and
/// where-clause via `split_for_impl` and adds **no** `StableName` bound (the const
/// does not depend on the type parameters). The default name is the bare identifier
/// (`"Wrapper"`, never `"Wrapper<T>"` — `<>` is not a well-formed stable-name
/// character); a type needing *per-monomorphisation* distinct names must write the
/// `impl StableName` by hand (the always-available fallback).
///
/// # Well-formedness
///
/// A default identifier is always a well-formed stable name (ASCII letters/digits
/// and `_`). An **override** value is validated for well-formedness at the
/// whole-pipeline **assembly** check (the authoritative enforcement point), never
/// here — the derive adds no second check.
///
/// (These `dagr_core` paths are plain code, not intra-doc links: this build-time
/// proc-macro crate does not depend on `dagr_core`, so its rustdoc cannot resolve
/// links into it.)
#[proc_macro_derive(StableName, attributes(stable_name))]
pub fn stable_name(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    expand_stable_name(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// The stable-name string literal a `#[derive(StableName)]` emits: the override from
/// a single `#[stable_name = "…"]` helper attribute if present, else the type's own
/// identifier as a source-level literal. A malformed helper attribute (missing/
/// non-string value, list form, or more than one) is a spanned error naming the
/// accepted grammar, mirroring [`parse_dag_name`].
fn stable_name_literal(input: &DeriveInput) -> syn::Result<syn::LitStr> {
    let grammar = "#[stable_name = \"…\"] takes exactly one string-literal value \
                   (the author-declared stable name); omit it to default to the type name";

    let mut found: Option<syn::LitStr> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("stable_name") {
            continue;
        }
        // The sole accepted form is `#[stable_name = "…"]` — a name-value pair whose
        // value is a string literal. A list (`#[stable_name(…)]`) or a bare path
        // (`#[stable_name]`) each map to a clear, spanned message.
        let Meta::NameValue(nv) = &attr.meta else {
            return Err(syn::Error::new(attr.meta.span(), grammar));
        };
        let value = match &nv.value {
            Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) => s.clone(),
            other => return Err(syn::Error::new(other.span(), grammar)),
        };
        // A second `#[stable_name = …]` is an authoring error: one override only.
        if found.is_some() {
            return Err(syn::Error::new(
                attr.span(),
                "duplicate `#[stable_name = \"…\"]`: a type declares at most one stable name",
            ));
        }
        found = Some(value);
    }

    Ok(found.unwrap_or_else(|| {
        // The default: the type's own identifier, baked as a source-level string
        // literal at expansion time. This is byte-stable across toolchains by
        // construction — it is the author's source token, exactly like a
        // hand-written `const STABLE_NAME: &str = "Foo"`. NEVER derive this from
        // `std::any::type_name`, whose format is not a cross-toolchain stability
        // guarantee (that would silently break the determinism fixtures).
        syn::LitStr::new(&input.ident.to_string(), input.ident.span())
    }))
}

/// Emit `impl StableName for #ident { const STABLE_NAME = #name; }`, reproducing the
/// type's own generics and where-clause (no added bound — the const is
/// type-parameter-independent). Infallible once [`stable_name_literal`] resolved the
/// name.
fn expand_stable_name(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = stable_name_literal(input)?;
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::dagr_core::stable_name::StableName
            for #ident #ty_generics #where_clause
        {
            const STABLE_NAME: &'static str = #name;
        }
    })
}
