//! Compile-pass: the four input arities the macro accepts, and the `type Input`
//! each infers.
//!
//! - zero dep args  -> `type Input = ()`
//! - one dep arg    -> the **bare** `T` (never `(T,)`)
//! - two dep args   -> `(A, B)`
//! - eight dep args -> the 8-tuple (the `MAX_INPUT_ARITY` ceiling)
//!
//! Each `assert_input` call is a *compile-time* proof that the generated
//! `type Input` matches the arity rule: it only type-checks when the bound holds.

use dagr_core::task;
use dagr_core::task::{RunContext, Task};
use dagr_core::TaskError;

/// Zero-input source: `type Input = ()`.
struct Source;

#[task]
impl Source {
    async fn run(&mut self, _input: ()) -> Result<u64, TaskError> {
        Ok(0)
    }
}

/// Single input, delivered **bare** (`type Input = u64`, never `(u64,)`).
struct Single;

#[task]
impl Single {
    async fn run(&mut self, input: u64) -> Result<u64, TaskError> {
        Ok(input * 2)
    }
}

/// Two inputs: `type Input = (u64, String)`, destructured by the tuple pattern.
struct Two;

#[task]
impl Two {
    async fn run(&mut self, (n, s): (u64, String)) -> Result<usize, TaskError> {
        Ok(s.len() + n as usize)
    }
}

/// Eight inputs (the ceiling): `type Input` is the 8-tuple.
struct Eight;

#[task]
impl Eight {
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &mut self,
        (a, b, c, d, e, f, g, h): (u8, u8, u8, u8, u8, u8, u8, u8),
    ) -> Result<u64, TaskError> {
        Ok(u64::from(a) + u64::from(b) + u64::from(c) + u64::from(d)
            + u64::from(e) + u64::from(f) + u64::from(g) + u64::from(h))
    }
}

/// Compile-time proof that `T::Input` is exactly `I`.
fn assert_input<T: Task<Input = I>, I>() {}

fn main() {
    assert_input::<Source, ()>();
    assert_input::<Single, u64>();
    assert_input::<Two, (u64, String)>();
    assert_input::<Eight, (u8, u8, u8, u8, u8, u8, u8, u8)>();

    // The `ctx`-detection also holds across arities: `Source` above omits `ctx`;
    // a sibling that *takes* one is exercised in `execution_class.rs`.
    let _ = (Source, Single, Two, Eight);
}
