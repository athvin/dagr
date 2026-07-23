// UI compile-failure fixture — ticket T5 (018), case `typed_handle_wrong_arity`.
//
// PROVES (C3): binding a DIFFERENT NUMBER of handles than the task declares is
// a COMPILE error. A task that declares it consumes exactly two inputs
// `(Alpha, Beta)` is bound ONE handle; the sealed `Deps` trait's
// `Inputs = T::Input` bound is unsatisfied, so the mis-wiring fails to compile.
// It is wired to the same T8 UI harness (crates/core/tests/ui.rs) that the full
// wiring compile-fail suite (T12) reuses: a sibling `.stderr` names the
// substrings the diagnostic must contain, and the harness asserts this sample
// FAILS to compile under the pinned toolchain (C28).
//
// This is a THROWAWAY, intentionally NON-COMPILING SKETCH — NOT a use of dagr's
// real authoring API (typed handles land in T10, the real binding in T11). It
// models only the settled T5 dependency-encoding: a sealed positional trait
// (`Deps`) maps a handle tuple to the task's declared input tuple, so COUNT,
// ORDER, and TYPES are all compile-checked. Arity mismatch surfaces here as an
// associated-type mismatch (E0271) naming the supplied vs required tuple.

use std::marker::PhantomData;

#[derive(Clone, Copy)]
struct NodeId(u32);

// Handle carries identity + the value's type; PhantomData<fn() -> T> keeps it
// Copy/Send/Sync regardless of T (see the ADR's handle-encoding decision).
struct Handle<T> {
    id: NodeId,
    _t: PhantomData<fn() -> T>,
}
impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Handle<T> {}

// Sealed positional binding: a handle tuple delivers a tuple of value types.
trait Deps {
    type Inputs;
}
impl<A> Deps for Handle<A> {
    type Inputs = A; // single input is the bare value `A`, never `(A,)`
}
impl<A, B> Deps for (Handle<A>, Handle<B>) {
    type Inputs = (A, B);
}

trait Task {
    type Input;
    type Output;
}
struct Flow {
    next: u32,
}
impl Flow {
    fn register<T, D>(&mut self, _task: T, _deps: D) -> Handle<T::Output>
    where
        T: Task,
        D: Deps<Inputs = T::Input>,
    {
        let id = NodeId(self.next);
        self.next += 1;
        Handle {
            id,
            _t: PhantomData,
        }
    }
    fn source<T: Task<Input = ()>>(&mut self, _task: T) -> Handle<T::Output> {
        let id = NodeId(self.next);
        self.next += 1;
        Handle {
            id,
            _t: PhantomData,
        }
    }
}

struct Alpha;
struct Beta;
struct Bytes;
struct MakeAlpha;
impl Task for MakeAlpha {
    type Input = ();
    type Output = Alpha;
}
struct MakeBeta;
impl Task for MakeBeta {
    type Input = ();
    type Output = Beta;
}
// Declares it consumes EXACTLY TWO inputs.
struct ConsumeTwo;
impl Task for ConsumeTwo {
    type Input = (Alpha, Beta);
    type Output = Bytes;
}

fn main() {
    let mut flow = Flow { next: 0 };
    let a: Handle<Alpha> = flow.source(MakeAlpha);
    let _b: Handle<Beta> = flow.source(MakeBeta);
    // Bind ONE handle where TWO are declared: `Handle<Alpha>: Deps<Inputs =
    // (Alpha, Beta)>` is unsatisfied — a wrong-arity binding is a compile error.
    let _bad = flow.register(ConsumeTwo, a);
}
