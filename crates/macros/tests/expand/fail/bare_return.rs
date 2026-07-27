//! Compile-fail: a `run` returning a **bare** non-`Result` type (`-> u64`
//! instead of `-> Result<u64, TaskError>`). The macro rejects it with a
//! `compile_error!` naming the required `Result<T, TaskError>` shape — the
//! failure channel is explicit, and this snapshot pins that message.

use dagr_core::task;

struct BareReturn;

#[task]
impl BareReturn {
    // Missing the `Result<_, TaskError>` wrapper — the failure channel is not
    // expressible, so the macro rejects it.
    async fn run(&mut self, input: u64) -> u64 {
        input * 2
    }
}

fn main() {}
