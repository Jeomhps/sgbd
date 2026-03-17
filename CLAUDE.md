# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all unit tests (pytest.ini sets PYTHONPATH=. and testpaths=tests automatically)
pytest

# Run a specific test file
pytest tests/operators/test_aggregate_operator.py

# Run a specific test function
pytest tests/operators/test_join_operator.py::TestJoinOperator::test_single_match

# Run manual integration tests
PYTHONPATH=. python3 tests/manual/TestSQL.py
PYTHONPATH=. python3 tests/manual/TestIndex.py
```

## Architecture

MiniSGBD is a relational database engine built around the **Operator Pipeline Pattern**: every operator implements `open() / next() / close()` and operators chain lazily.

### Operator Interface

```python
class MyOperator(Instrumentation, Operateur):
    def open(self)  -> None        # initialize, open children
    def next(self)  -> Tuple|None  # return next row, None = exhausted
    def close(self) -> None        # release resources, close children
```

`Instrumentation` (mixed in via multiple inheritance) tracks `tuplesProduits`, `memoire`, and `time` without polluting operator logic. Every `__init__` must call `super().__init__("OperatorName" + str(Instrumentation.number))` and increment `Instrumentation.number`.

### Execution layers

```
SQL string
  └─ sql/Lexer.py          → token stream (TokenType enum, Token dataclass)
  └─ sql/Parser.py         → AST (ColumnRef, AggExpr, Condition, SelectQuery)
  └─ sql/Planner.py        → operator tree  (QueryPlanner.plan → Operateur)
  └─ sql/Executor.py       → drives the tree (execute / execute_and_print)
  └─ sql/Catalog.py        → name → table registry (TableMemoire + TableDisque)
```

### Storage

| Class | File | Description |
|-------|------|-------------|
| `TableMemoire` | `core/TableMemoire.py` | In-memory list of `Tuple` objects |
| `TableDisque` | `core/TableDisque.py` | Binary file: header `II` (table_size, tuple_size) + data `i…`; block I/O + LRU cache |
| `IndexDisque` | `core/IndexDisque.py` | Subclass of `TableDisque` with `tuple_size=2`; stores `(key, tuple_idx)` pairs; adds `write_entries()`, `get_entry()`, `scan_range()` |

`TableDisque` file layout: `struct.pack('II', table_size, tuple_size)` header (8 bytes) then `table_size × tuple_size × 4` bytes (each value is a signed 32-bit int).

### Operators

| File | Operator | Notable behaviour |
|------|----------|-------------------|
| `core/FullScanTableMemoire.py` | `FullScanTableMemoire` | Source for in-memory tables |
| `core/FullScanTableDisque.py` | `FullScanTableDisque` | Source for disk tables; `open()` calls `table.open()` |
| `operators/Project.py` | `Project(child, cols)` | Creates a new `Tuple` per row |
| `operators/Restrict.py` | `Restrict(child, col, val, op)` | op ∈ `==, !=, >, <, >=, <=` |
| `operators/Join.py` | `Join(left, right, l_col, r_col)` | Nested-loop; caches right side in `open()` |
| `operators/HashJoin.py` | `HashJoin(left, right, l_col, r_col)` | Hash-build on right, probe with left |
| `operators/Aggregate.py` | `Aggregate(child, agg_col, func, group_cols)` | Two-phase (collect all → compute); `result_returned` flag prevents infinite output |
| `operators/IndexScan.py` | `IndexScan(table, index, value, op, high)` | Queries index → `_indices` list → `table.get_tuple(i)` on `next()` |
| `operators/IndexNestedLoopJoin.py` | `IndexNestedLoopJoin(left, right_table, right_index, left_col, op, high)` | Per left tuple: `right_index.search(key)` → fetch right rows directly; no right-side caching; iterative `next()` (no recursion) |
| `operators/SortMergeJoin.py` | `SortMergeJoin(left, right, left_col, right_col)` | Phase 1: collect + sort both sides by join column; Phase 2: two-pointer merge — left scanned once, right group replayed for duplicate left keys |

### Indexes

All three index types store their entries on disk via `IndexDisque`. After `build()` the in-memory tree/buckets are discarded; `search()` reads from disk with the LRU cache inherited from `TableDisque`. A companion `.dir` file stores the metadata (bucket directory or B+ tree stats).

| Class | File | `search()` | `range_search()` |
|-------|------|-----------|-----------------|
| `StaticHashIndex` | `index/StaticHashIndex.py` | `hash(key) % nb_buckets` → disk scan of bucket slice | — |
| `DynamicHashIndex` | `index/DynamicHashIndex.py` | Extendible hashing, directory of `2^global_depth` entries | — |
| `BPlusTreeIndex` | `index/BPlusTreeIndex.py` | Binary search on sorted IndexDisque entries | Scan from lower-bound position |

### Column resolution in the Planner

After joining N tables the combined tuple is a flat concatenation:

```
[ T1.col0 … T1.colK | T2.col0 … T2.colM | … ]
  offset[T1]=0        offset[T2]=nb_att(T1)
```

Column-name conventions:
- `T1.A2` → `offsets['T1'] + 1`  (A-notation is 1-indexed)
- `A2`    → `1`  (no table qualifier)
- `2`     → `2`  (bare integer, 0-based)

### Adding a new operator

1. Create `operators/MyOp.py`, inherit `Instrumentation` and `Operateur`.
2. Call `super().__init__("MyOp" + str(Instrumentation.number))` and increment `Instrumentation.number`.
3. Implement `open()`, `next()`, `close()`.
4. `open()` and `next()` must bracket work with `self.start()` / `self.stop()`; call `self.produit(t)` when yielding a tuple.
5. Add a test in `tests/operators/`.
