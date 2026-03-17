# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
make test          # or: python3 run.py all

# Run specific test suites
make project       # Project operator tests
make restrict      # Restrict operator tests
make chained       # Chained restrict tests
make join          # Join operator tests
make aggregate     # Aggregate operator tests

# Clean Python cache
make clean
```

Tests set `PYTHONPATH` to the project root automatically — no manual setup needed.

## Architecture

MiniSGBD is a relational database engine built around the **Operator Pipeline Pattern**: operators chain together forming a query execution plan evaluated lazily via an iterator protocol.

### Operator Interface (core/Operateur.py)

Every operator implements three methods:
- `open()` — initialize and prepare
- `next()` — return next `Tuple` or `None` when exhausted
- `close()` — release resources

All operators also inherit `Instrumentation` which tracks tuple count, memory, and execution time without polluting operator logic.

### Operator Chain Example

```python
# SELECT a0, a2 FROM table WHERE a1 > 50
scan = FullScanTableMemoire(table)
filtered = Restrict(scan, col=1, val=50, op='>')
projected = Project(filtered, [0, 2])
```

### Operator Implementations

| File | Operator | Key Detail |
|------|----------|------------|
| `core/FullScanTableMemoire.py` | Table scan | Baseline source operator |
| `operators/Project.py` | Column projection | Produces new `Tuple` with selected columns |
| `operators/Restrict.py` | Row filter | Supports `==`, `!=`, `>`, `<`, `>=`, `<=` |
| `operators/Join.py` | Nested loop join | Caches right table in memory on `open()` |
| `operators/Aggregate.py` | Aggregation | Two-phase: collect all, then compute; supports GROUP BY |

### Data Model

- `TableMemoire` — in-memory table with list of `Tuple` objects
- `Tuple` — fixed-size value array (`val: list`, `size: int`)
- `TableMemoire.randomize(tuplesize, val_range, tablesize)` — generates random test data

### Adding a New Operator

1. Create file in `operators/`
2. Inherit from both `Instrumentation` and `Operateur`
3. Accept child operator(s) in constructor
4. Implement `open()`, `next()`, `close()`
5. Add a test file in `tests/` and a `make` target in the `Makefile`
