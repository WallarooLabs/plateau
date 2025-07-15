# Migration Plan: arrow2 to arrow-rs

This document outlines the plan for migrating the Plateau repository from
arrow2 to arrow-rs. The migration will be performed gradually, crate by crate,
to ensure a smooth transition and easier code review.

This will be a long and complex migration. We may be partially done while
reading this doc. We should resume from where we left off, attempt to complete
the currently pending task, and then wait for review.

## Migration Strategy

For each crate, we will first:

1. Create a copy of the crate with `-arrow-rs` suffix (e.g., `plateau-transport` => `plateau-transport-arrow-rs`). For example:
  - `cp -r transport arrow-rs/transport`
  - Update arrow-rs/transport/Cargo.toml with the new name (`plateau-transport-arrow-rs`)
  - Update Cargo.toml to add `arrow-rs/transport`
2. Update this file to indicate this part of the migration is done.
3. DO NOT MAKE ANY CHANGES UNTIL THE CLEAN BASELINE HAS BEEN COMMITTED.
4. Make an immediate commit of the copied crate to establish a clean base for comparison. Example:
  - `git add Cargo.toml`
  - `git add arrow-rs/transport`
  - `git commit -m "Initial copy transport => arrow-rs/transport"`
5. STOP and wait for review.

Then, we will iterate until the crate is ready. We MUST NOT CHANGE OR REMOVE
tests (beyond getting them to work with arrow-rs). We MUST NOT CHANGE THE ORDER
OR NAMES of tests:

1. Update arrow2 to arrow-rs
2. Fix any obvious compiler errors. Example:
  - `cargo check -p plateau-transport-arrow-rs`
3. Ensure that all tests from the old crate remain, in the same order, with the same names. Example:
  - `cargo test -p plateau-transport`
  - `cargo test -p plateau-transport-arrow-rs`
4. If all tests are passing, and all test (names) from the old crate exactly match the
   migrated crate (including order), update this doc to indicate the task is done.
5. Run a final `cargo fmt` to remove any formatting issues.
5. STOP and wait for review.

## Dependency Order Analysis

Based on the repository structure, the migration order is determined by dependencies between crates:

- **transport**: Base library with direct arrow2 dependencies, no internal dependencies.
- **client**: Depends on transport.
- **test**: Depends on transport, client, and server.
- **server**: Depends on transport and client.
- **cli**: Depends on transport and client.
- **bench**: Depends on server and client.
- **plateau**: Depends on server (which brings in the rest).

## Migration Tasks

### Phase 1: Base Libraries

- [ ] **transport-arrow-rs**
  - [ ] Create copy of transport
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality
  
### Phase 2: Client Libraries

- [ ] **client-arrow-rs**
  - [ ] Create copy of client
  - [ ] Update dependencies to use transport-arrow-rs
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality

### Phase 3: Test Infrastructure

- [ ] **test-arrow-rs**
  - [ ] Create copy of test
  - [ ] Update dependencies to use transport-arrow-rs and client-arrow-rs
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality

### Phase 4: Server Implementation

- [ ] **server-arrow-rs**
  - [ ] Create copy of server
  - [ ] Update dependencies to use transport-arrow-rs and client-arrow-rs
  - [ ] Update references to parquet2 if necessary
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality

### Phase 5: CLI Tool

- [ ] **cli-arrow-rs**
  - [ ] Create copy of cli
  - [ ] Update dependencies to use transport-arrow-rs and client-arrow-rs
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality

### Phase 6: Benchmarking

- [ ] **bench-arrow-rs**
  - [ ] Create copy of bench
  - [ ] Update dependencies to use server-arrow-rs and client-arrow-rs
  - [ ] Update arrow2 to arrow-rs, verify tests and functionality

### Phase 7: Main Application

- [ ] **plateau-arrow-rs**
  - [ ] Create copy of plateau
  - [ ] Update dependencies to use server-arrow-rs
  - [ ] Verify tests and functionality

### Phase 8: Integration

- [ ] Update workspace Cargo.toml to include new arrow-rs crates
- [ ] Update workspace patch section to remove arrow2 dependencies
- [ ] Final integration testing

## Migration Notes

### Troubleshooting Docs

We can pull the docs for any crate we're having issues with using these commands (e.g. for arrow2):

```
cargo doc -p arrow2 # can add multiple -p args here
rm -rf doc
cp -r target/doc/ doc
```

### Key API Differences

- arrow-rs uses a different package structure than arrow2
- arrow-rs API may have different method names and parameters
- Data type handling might differ between arrow2 and arrow-rs
- Serialization/deserialization approaches may need adjustment
- Memory management might be different

### JSON Serialization (pandas-record format)

The repository uses the pandas-record JSON format for serialization. When migrating from arrow2 to arrow-rs, the serialization code needs to be updated to use the equivalent arrow-rs APIs. An example implementation using arrow-rs is available in `examples/serde.rs` along with sample data in `examples/records.json`.

Sample pandas-record JSON format:
```json
[
    {
        "temp": 21.0,
        "pressure": 3.0,
        "name": "sensor_1",
        "measurements": [[1.1, 2.2, 3.3], [4.4, 5.5]]
    },
    {
        "temp": 22.0,
        "pressure": 4.0,
        "name": "sensor_2", 
        "measurements": [[6.6, 7.7], [8.8, 9.9, 10.0]]
    }
]
```

Key changes for JSON serialization:

- arrow2 uses its own JSON serialization modules, while arrow-rs uses `arrow::json` and `arrow_json` modules
- Schema definition syntax differs between arrow2 and arrow-rs
- arrow-rs uses `ReaderBuilder` and `ArrayWriter` for JSON serialization/deserialization
- Field creation and nesting structure has a different syntax in arrow-rs
- Record batch handling follows a different pattern

When converting JSON serialization code, pay special attention to:

- Schema definition and field type mapping
- Nested data structures (lists, structs)
- The serialization/deserialization workflow
- Error handling differences

### Common Patterns

(This section will be updated as we progress through the migration)

- **Type conversions**: Document common type conversion patterns
- **API replacements**: Document common method replacements
- **Error handling**: Note differences in error types and handling
- **Performance considerations**: Document any performance implications

### Testing Considerations

- Ensure compatibility with existing data formats
- Verify serialization/deserialization correctness
- Check for performance regressions
- Verify memory usage patterns
- Ensure all public APIs maintain backward compatibility where possible

### References

- [arrow-rs Documentation](https://docs.rs/arrow/latest/arrow/)
- [arrow2 Documentation](https://docs.rs/arrow2/latest/arrow2/)
- [Apache Arrow Format](https://arrow.apache.org/docs/format/Columnar.html)
