from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Join import Join

print("=== Testing Join Operator ===")

# Create left table (employees: id, name, dept_id)
left_table = TableMemoire(3)
print("Left Table (Employees): id, name, dept_id")

# Add specific tuples to left table
tuple1 = Tuple(3)
tuple1.val = [1, 101, 10]  # id=1, name=101, dept_id=10
left_table.valeurs.append(tuple1)

tuple2 = Tuple(3)
tuple2.val = [2, 102, 20]  # id=2, name=102, dept_id=20
left_table.valeurs.append(tuple2)

tuple3 = Tuple(3)
tuple3.val = [3, 103, 10]  # id=3, name=103, dept_id=10
left_table.valeurs.append(tuple3)

for i, t in enumerate(left_table.valeurs):
    print(f"  Employee {i}: {t}")

print()

# Create right table (departments: dept_id, dept_name, budget)
right_table = TableMemoire(3)
print("Right Table (Departments): dept_id, dept_name, budget")

tuple4 = Tuple(3)
tuple4.val = [10, 201, 100000]  # dept_id=10, dept_name=201, budget=100000
right_table.valeurs.append(tuple4)

tuple5 = Tuple(3)
tuple5.val = [20, 202, 200000]  # dept_id=20, dept_name=202, budget=200000
right_table.valeurs.append(tuple5)

tuple6 = Tuple(3)
tuple6.val = [30, 203, 150000]  # dept_id=30, dept_name=203, budget=150000
right_table.valeurs.append(tuple6)

for i, t in enumerate(right_table.valeurs):
    print(f"  Department {i}: {t}")

print()

# Create scans
left_scan = FullScanTableMemoire(left_table)
right_scan = FullScanTableMemoire(right_table)

# Create join: join on left.dept_id (col 2) == right.dept_id (col 0)
# SQL equivalent: SELECT * FROM employees JOIN departments ON employees.dept_id = departments.dept_id
join = Join(left_scan, right_scan, 2, 0)  # left_col=2, right_col=0

print("Join Results (Employees JOIN Departments on dept_id):")
print("Format: employee_id, employee_name, dept_id, dept_id, dept_name, budget")
print("-" * 80)

join.open()
result_count = 0
while True:
    result = join.next()
    if result is None:
        break
    print(f"Result {result_count + 1}: {result}")
    result_count += 1

join.close()

print("-" * 80)
print(f"✅ Join produced {result_count} results")
print(f"Expected: 3 results (Employee 1 & 3 join with Dept 10, Employee 2 joins with Dept 20)")
print()
print("Join Statistics:")
print(join)

# Test with no matches
print()
print("=" * 60)
print("Testing Join with No Matches:")

# Create tables with no matching join keys
left_table2 = TableMemoire(2)
tuple7 = Tuple(2)
tuple7.val = [1, 100]
left_table2.valeurs.append(tuple7)

right_table2 = TableMemoire(2)
tuple8 = Tuple(2)
tuple8.val = [2, 200]  # Different key
right_table2.valeurs.append(tuple8)

left_scan2 = FullScanTableMemoire(left_table2)
right_scan2 = FullScanTableMemoire(right_table2)
join2 = Join(left_scan2, right_scan2, 0, 0)

print("Left table: [[1, 100]]")
print("Right table: [[2, 200]]")
print("Join on col0 == col0 (should produce 0 results)")

join2.open()
no_match_count = 0
while True:
    result = join2.next()
    if result is None:
        break
    no_match_count += 1

join2.close()
print(f"Results: {no_match_count} (expected: 0)")
print(join2)

print()
print("🎉 Join operator testing completed!")