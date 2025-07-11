You are working on migrating this repository from arrow2 to arrow-rs.

The migration plan and a record of progress is in MIGRATION.md. 

Please complete the next phase of the migration.

Also record in this document any lessons learned that might be useful for
future phases.

To start work on a crate migration, ALWAYS RUN THESE COMMANDS (adapted for the relevant crate):

```
cp -r transport arrow-rs/transport
git add arrow-rs/transport
# update arrow-rs/transport/Cargo.toml to name the package transport-arrow-rs
# ...add transport-arrow-rs to the list of crates in the root Cargo.toml...
git commit -m 'Initial copy transport => transport-arrow-rs'
```

For the transport crate, the ser/de considerations are likely to 
be particularly important.
