# Contributing to Invar

Thanks for your interest in Invar. This project is early, and contributions of all
sizes; bug reports, compatibility gap reports, docs fixes, and code are genuinely
welcome. If you have a more pointed question, it might be better to ask directly
in our [Discord server](https://discord.gg/WWE4PWVnb2).

## Ways to contribute

- **Report a bug.** Use the bug report template.
- **Report or request a command.** If a Redis or MongoDB command is missing, behaves
  incorrectly, or you want to see it prioritized, use the compatibility template. See
  [COMPATIBILITY.md](COMPATIBILITY.md) for current status — ✅ implemented, 🚧 not yet
  implemented, 🚫 no plan to implement.
- **Write code.** See below.

## Before you start on code

For anything beyond a small, obvious fix, please open an issue (or comment on an
existing one) before starting work. Invar is still finding its shape in a few areas:
scripting, streams, and where we draw the line on compatibility vs divergence. An early conversation
saves everyone time versus a large PR that appears out of nowhere without any context.

## Development setup

Invar is a Cargo-based Rust project.

**Build:**
```
cargo build
```

**Run** (the storage backend is a mandatory subcommand):
```
./target/debug/invar --backend fjall --redis
```
This starts a Fjall-backed (local disk) daemon listening on `:6379`. `--redis` is
currently the only supported protocol flag.

**Test:**
```
cargo test --workspace       # Rust unit tests
./run-tests.sh                # unit + integration tests (integration suite uses Deno/TypeScript)
```

Run `cargo clippy` and make sure it's clean for any code you touch.

For a deeper look at project structure and the on-disk key layout, see the
[Building Invar](https://docs.hardpoint.dev) architecture page. [AGENTS.md](AGENTS.md)
remains the source of truth if anything here drifts out of date.

## A few principles that matter here specifically

- **Correctness before everything.** Invar is not an in-memory data store; unintended
  data loss or modification is never acceptable. When in doubt, favor the more
  conservative behavior.
- **Minimize unrelated code churn.** If a change pulls in a significant amount of
  adjacent refactoring, flag it in the PR description rather than bundling it silently.
- **Don't speculate on protocol/API behavior.** If Redis's or MongoDB's documented
  behavior is ambiguous for a given edge case, open an issue to discuss rather than
  guessing at what "feels right."

## Branching strategy

Branch off the latest `master`. Open a PR with a clear description once you're ready
and wait for checks to pass — direct pushes to `master` are blocked.

## Code style

- Run `cargo fmt` and make sure `cargo clippy` is clean before opening a PR; CI checks
  both.
- Keep PRs scoped to one logical change. Large, multi-purpose PRs are harder to review
  and more likely to stall.

## Contributing a command implementation

This is one of the highest-value ways to help right now. Before starting:

1. Check [COMPATIBILITY.md](COMPATIBILITY.md) for the command's current status.
2. For 🚧 ("not yet implemented") commands — go ahead, open a PR. Use the
   *Compatibility* PR template so reviewers get the context they need up front.
3. For 🚫 ("no plan to implement") commands — open an issue first using the
   *Compatibility gap* issue template and make the case. Some of these are deliberate
   design decisions (e.g. cluster management, which doesn't map onto Invar's
   single-writer model); others are just under-prioritized, and a real use case can
   change that.
4. Any command PR should update COMPATIBILITY.md's status and notes for the row it
   touches, and add test coverage (unit tests, plus the Deno integration suite where
   relevant).

## Submitting a pull request

1. Fork the repo and create a branch off `master`.
2. Make your change, with tests where applicable.
3. Open a PR using the appropriate template — the default for general changes, or
   *Compatibility* for command implementations — it'll prompt you for what reviewers
   need.
4. Expect review comments; this is a young, mostly-solo-maintained project, so response
   time may vary, but every PR gets a look.

## License

Invar is licensed under Apache 2.0. By contributing, you agree that your contributions
will be licensed under the same terms.

## Code of conduct

This project follows a code of conduct — see [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
Be kind; this is a small project run by one person for now, and good-faith engagement
goes a long way.