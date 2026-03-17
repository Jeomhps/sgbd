# 🚀 MiniSGBD SQL Interpreter

An interactive SQL interpreter for the MiniSGBD database system that works with disk-based tables.

## Features

- **Load tables from disk**: Load `.dat` files created by `TableDisque`
- **Interactive REPL**: Type SQL queries and see results immediately
- **Query execution**: Full SQL query processing with parsing, planning, and execution
- **Pretty output**: Formatted results with execution plans
- **Disk-based**: Works with persistent table storage

## Quick Start

### 1. Create Sample Data

```bash
uv run python create_sample_data.py
```

This creates:
- `employees.dat`: Employee data (id, name, salary, dept_id)
- `departments.dat`: Department data (dept_id, name)

### 2. Launch the Interpreter

```bash
uv run python sql_interpreter.py
```

### 3. Load Tables

```sql
LOAD employees employees.dat
LOAD departments departments.dat
```

### 4. Run Queries

```sql
-- Simple selection
SELECT * FROM employees WHERE 2 > 50000

-- Join with projection
SELECT employees.0, employees.1, employees.2, departments.1 
FROM employees, departments 
WHERE employees.3 = departments.0

-- Aggregation
SELECT AVG(2) FROM employees GROUP BY 3
```

## SQL Syntax Support

### Basic Queries

```sql
SELECT * FROM table_name
SELECT col1, col2 FROM table_name
SELECT 0, 2, 4 FROM table_name  -- 0-indexed column numbers
```

### Filtering

```sql
SELECT * FROM table WHERE col > value
SELECT * FROM table WHERE col = value
SELECT * FROM table WHERE col < value
```

### Joins

```sql
SELECT * FROM table1, table2 WHERE table1.col = table2.col
SELECT t1.0, t2.1 FROM table1 t1, table2 t2 WHERE t1.2 = t2.0
```

### Aggregations

```sql
SELECT SUM(col) FROM table
SELECT AVG(col) FROM table
SELECT MIN(col) FROM table
SELECT MAX(col) FROM table
SELECT COUNT(col) FROM table

-- With GROUP BY
SELECT SUM(col1) FROM table GROUP BY col2
```

## Commands

| Command | Description |
|---------|-------------|
| `LOAD <name> <path>` | Load table from disk file |
| `TABLES` | List loaded tables |
| `HELP` or `?` | Show help information |
| `EXIT`, `QUIT`, or `Q` | Exit the interpreter |

## Example Session

```
🚀 MiniSGBD SQL Interpreter
Type 'help' for commands, 'exit' to quit

miniSGBD> LOAD employees employees.dat
✅ Loaded table 'employees' from employees.dat
   Schema: 4 columns, 5 rows

miniSGBD> LOAD departments departments.dat
✅ Loaded table 'departments' from departments.dat
   Schema: 2 columns, 3 rows

miniSGBD> SELECT * FROM employees WHERE 2 > 50000

📊 Execution Plan:
Restrict(col=2, val=50000, op='>')
  └─ FullScan(EMPLOYEES)

2	102	60000	10
3	103	55000	20
5	105	70000	30

(3 rows)

miniSGBD> SELECT employees.0, employees.1, employees.2, departments.1 
FROM employees, departments 
WHERE employees.3 = departments.0

📊 Execution Plan:
Project(cols=[0, 1, 2, 5])
  └─ Join(left_col=3, right_col=0)
    └─ FullScan(EMPLOYEES)
    └─ FullScan(DEPARTMENTS)

1	101	50000	1001
2	102	60000	1001
3	103	55000	1002
4	104	45000	1002
5	105	70000	1003

(5 rows)

miniSGBD> EXIT
👋 Goodbye!
```

## How It Works

### Architecture

```
SQL Query
    ↓
Parser (AST)
    ↓
Planner (Operator Tree)
    ↓
Executor (Results)
```

1. **Parser**: Converts SQL text to Abstract Syntax Tree (AST)
2. **Planner**: Converts AST to operator tree (FullScan → Join → Restrict → Project)
3. **Executor**: Traverses operator tree to produce results

### Disk Integration

- Tables are loaded from `.dat` files created by `TableDisque`
- Uses memory-mapped I/O for efficient disk access
- Supports LRU caching for better performance

## Creating Your Own Data

To create your own `.dat` files:

```python
from core.TableDisque import TableDisque

# Create a new table
table = TableDisque("my_table.dat")
table.create(tuple_size=3, table_size=100, randomize=True)

# Or create with specific data
# (See create_sample_data.py for examples)
```

## Technical Details

### Column References

- **Numeric**: `0`, `1`, `2` (0-indexed global column positions)
- **Qualified**: `table.A1`, `table.A2` (1-indexed attribute notation)
- **Table aliases**: `SELECT e.0, d.1 FROM employees e, departments d`

### Data Types

- All values are integers (as per `TableMemoire` implementation)
- String values are represented as integer codes
- Future enhancement: Add proper type system

### Limitations

- No transaction support
- No indexes (full table scans only)
- Basic SQL subset only
- No subqueries
- No complex expressions in SELECT

## Future Enhancements

- Add CREATE TABLE support
- Implement INSERT/UPDATE/DELETE
- Add transaction support
- Implement indexes for faster lookups
- Extend SQL syntax support
- Add query optimization

## License

This SQL interpreter is part of the MiniSGBD educational project.

---

**Happy querying! 🎉**