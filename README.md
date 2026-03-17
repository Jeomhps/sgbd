# MiniSGBD - Mini Database System

A lightweight relational database system implementing core relational algebra operators.

## 🗂️ Project Structure

```
miniSGBD/
├── core/          # Base classes and data structures
├── operators/     # Database operators (Project, Restrict, Join, Aggregate)
├── tests/         # Unit tests (pytest)
│   ├── core/      # Core component tests
│   └── operators/ # Operator tests
├── manual_tests/  # Manual demonstration tests
├── pyproject.toml # Project configuration with uv dependencies
└── README.md      # This file
```

## 🚀 Features

### Database Operators

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| **Project** | Select specific columns | `SELECT col1, col2 FROM table` |
| **Restrict** | Filter rows by condition | `SELECT * FROM table WHERE col > 50` |
| **Join** | Combine tables (nested loop) | `SELECT * FROM A JOIN B ON A.id = B.id` |
| **HashJoin** | Combine tables (hash-based) | `SELECT * FROM A JOIN B ON A.id = B.id` |
| **Aggregate** | Compute aggregations | `SELECT SUM(col), AVG(col) FROM table` |

### Aggregation Functions

- `SUM` - Sum of values
- `AVG` - Average of values  
- `MIN` - Minimum value
- `MAX` - Maximum value
- `COUNT` - Count of rows
- `GROUP BY` - Grouped aggregations

## 🧪 Testing

### Unit Tests (CI/CD)
For automated testing and CI pipelines:
```bash
# Run all unit tests
uv run pytest tests

# Run specific test directory
uv run pytest tests/core/    # Core component tests
uv run pytest tests/operators/ # Operator tests

# Run with verbose output
uv run pytest tests -v

# Run and stop on first failure
uv run pytest tests -x
```

### Manual Tests (Demonstration)
For visual demonstration and learning:
```bash
# Run manual integration tests
python manual_tests/TestDiskIntegration.py
python manual_tests/TestProject.py
python manual_tests/TestRestrict.py
```

### Test Organization
```
tests/
├── core/          # Core component tests (Tuple, TableMemoire, etc.)
│   ├── test_tuple.py
│   ├── test_table_memoire.py
│   ├── test_full_scan_table_memoire.py
│   └── test_disk_table.py
└── operators/     # Operator tests (Project, Restrict, Join, etc.)
    ├── test_project_operator.py
    ├── test_restrict_operator.py
    ├── test_join_operator.py
    ├── test_aggregate_operator.py
    └── test_hashjoin.py

manual_tests/     # Manual demonstration tests
├── TestDiskIntegration.py
├── TestProject.py
├── TestRestrict.py
├── TestJoin.py
└── TestAggregate.py
```

## 📋 Usage Examples

### Basic Operations
```python
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Project import Project
from operators.Restrict import Restrict
from operators.Aggregate import Aggregate

# Create and populate table
table = TableMemoire.randomize(3, 100, 10)
scan = FullScanTableMemoire(table)

# Project operator (SELECT col0, col2)
project = Project(scan, [0, 2])

# Restrict operator (WHERE col1 > 50)
restrict = Restrict(scan, 1, 50, ">")

# Aggregate operator (SELECT AVG(col2))
agg = Aggregate(scan, 2, 'AVG')
```

### SQL-like Operations
```python
# SELECT SUM(sales) FROM orders
sum_agg = Aggregate(scan, 2, 'SUM')

# SELECT product_id, AVG(price) FROM products GROUP BY category
group_agg = Aggregate(scan, 3, 'AVG', [1])  # Group by column 1

# SELECT * FROM employees JOIN departments ON dept_id
join = Join(emp_scan, dept_scan, 2, 0)  # emp.dept_id = dept.id
```

## 🔧 Technical Details

### Operator Pipeline Pattern
All operators follow the same interface:
- `open()` - Initialize the operator
- `next()` - Get next tuple (returns `Tuple` or `None`)
- `close()` - Clean up resources

### Memory Management
- Operators track memory usage via `Instrumentation`
- Statistics include tuples produced, memory used, and execution time
- Proper resource cleanup in `close()` methods

### Type Safety
- Base classes include type hints
- Child classes inherit type signatures automatically
- Clean separation between operator logic and data structures

## 📚 Learning Resources

This project demonstrates:
- **Relational Algebra** fundamentals
- **Operator Pipeline** patterns
- **Memory Management** in data processing
- **Test-Driven Development** approach
- **SQL Query Execution** concepts

## 🤝 Contributing

1. Add new operators to `operators/` directory
2. Create corresponding tests in `tests/core/` or `tests/operators/`
3. Run `uv run pytest tests` to verify everything works
4. Ensure all tests pass before submitting changes

## 📊 Performance

### Join Algorithm Comparison

| Algorithm | Complexity | Memory Usage | Best For |
|-----------|------------|--------------|----------|
| **Nested Loop Join** | O(n×m) | O(1) | Small datasets, simple implementation |
| **Hash Join** | O(n+m) | O(m) | Large datasets, better performance |

### Hash Join Advantages
- **Faster**: O(n+m) vs O(n×m) for nested loop
- **Scalable**: Handles large datasets efficiently
- **Hash-based**: Uses hash table for O(1) lookups
- **Two-phase**: Build hash table, then probe

### Performance Monitoring
All operators include instrumentation:
- Execution time tracking
- Memory usage monitoring
- Tuple count statistics

Example output:
```
HashJoin1 -- tuples produits: 1000 -- mémoire utilisée: 48 -- Time: 0.0012
```
