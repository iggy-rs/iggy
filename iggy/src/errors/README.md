# What is this

`Error` variants are created from error table in a `sled` database
when running `cargo build` due to the custom `build.rs` at the projects
root.

Workflow:

1. Add your `Error` variant by updating `insert_errors` in `iggy/errors_repository.rs`
2. Delete the directory `iggy/errors_table` to ensure the database is rebuild in the next step.
3. Run `cargo build`.
4. You can use your new `Error` variant as usual.
