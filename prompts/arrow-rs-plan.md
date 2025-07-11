Create a migration plan for converting this repository from arrow2 to arrow-rs.

Put this plan in MIGRATION.md. We will use this document to capture knowledge
and track progress.

Create tasks for updating each crate in the appropriate order. Crates should
not be modified in-place, as this will break dependencies. We also do not want
to convert the entire repository at once, as that will be difficult to review.

Instead, we will:

- Create a copy of a crate whose dependencies have been ported (starting with
  crates with no dependencies). For example, `transport` ->
  `transport-arrrow-rs`.
- Immediately commit so we have a clean base of comparison.
- Make the necessary changes to ensure that new crate works, including
  verifying that `cargo test` and `cargo check` work and return no warnings.
- Move on to a new crate.

We'll need to do this for all crates in this repository.

MIGRATION.md should contain a list of these tasks that you can update as you
work, and a notes section for generally useful information you discover as
you complete this migration.
