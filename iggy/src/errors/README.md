# What is this

`Error` variants are created from error table in a SQLite database
when running `cargo build` due to the custom `build.rs` at the projects
root.

Workflow:

1. Add your `Error` variant including documentation to the error table by
   adding a value in the `error_table_db/insert_into.sql` script.
2. Run `cargo build`.
3. You can use your new `Error` variant as usual.

# FAQ

Q: Why a database?
A: This should help to centrally manage `Error` variants used by the application.
   The table easily reveals which `Error` belongs to which error code and vice versa.
   On contrast the generated `errors.rs` file contains the same information but spread
   out across the file. Using a DB browser it also becomes easier to order the different
   errors which for example helps to spot error message templates duplicates or see all codes
   from a 4XXX group. Another benefit is that the database can take care of integrity and constraints.
   It might also be easier to represent the errors differently in code if you decide to re-assemble
   everything differently (data versus representation).

Q: What are the downsides?
A: You have to adapt your workflow as a developer.

Q: How are changes tracked in Git?
A: You should do changes in `error_table_db/insert_into.sql` and commit them to Git.
   The actual upserts should be done by the build pipeline.
