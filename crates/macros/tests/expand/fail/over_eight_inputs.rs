//! Compile-fail: a task binding **more than eight** inputs. The macro adds no
//! second ceiling — an over-8 tuple flows through as `type Input` — and the
//! sealed `dagr_core::binding::Deps` trait (whose tuple impls stop at
//! `MAX_INPUT_ARITY = 8`) surfaces the curated `#[diagnostic::on_unimplemented]`
//! "too many inputs" error at the point the task is **wired** through
//! `RunnableFlow`. This snapshot pins that actionable, 8-input-ceiling message.

use dagr_core::task;

use dagr_cli::run_flow::RunnableFlow;

/// A source producing a `u8`, reused as nine upstreams.
struct Src;

#[task]
impl Src {
    async fn run(&mut self, _input: ()) -> Result<u8, dagr_core::TaskError> {
        Ok(1)
    }
}

/// Nine inputs — one past the ceiling. The macro accepts the 9-tuple as
/// `type Input`; the `Deps` cliff bites when it is bound below.
struct NineInputs;

#[task]
#[allow(clippy::type_complexity)]
impl NineInputs {
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &mut self,
        (a, b, c, d, e, f, g, h, i): (u8, u8, u8, u8, u8, u8, u8, u8, u8),
    ) -> Result<u64, dagr_core::TaskError> {
        Ok(u64::from(a) + u64::from(b) + u64::from(c) + u64::from(d) + u64::from(e)
            + u64::from(f) + u64::from(g) + u64::from(h) + u64::from(i))
    }
}

fn main() {
    let mut flow = RunnableFlow::new();
    let s = flow.register_source("src", Src);
    // Bind nine upstream handles as a 9-tuple: past the arity ceiling, so the
    // `Deps` `on_unimplemented` diagnostic fires here.
    let _ = flow.register::<NineInputs, _>(
        "nine",
        NineInputs,
        (s, s, s, s, s, s, s, s, s),
    );
}
