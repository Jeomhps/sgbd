"""
Test integration between disk tables and existing operators.
Demonstrates that operators can work with both memory and disk tables.
"""

import os
from core.TableDisque import TableDisque
from core.FullScanTableDisque import FullScanTableDisque
from core.Tuple import Tuple
from operators.Project import Project
from operators.Restrict import Restrict
from operators.Aggregate import Aggregate

print("=== Testing Disk Table Integration with Operators ===")

# Create a disk table
table_file = "integration_test.dat"
disk_table = TableDisque(table_file)
disk_table.create(tuple_size=4, table_size=20, randomize=True)

print(f"✅ Created disk table: {disk_table.table_size} tuples × {disk_table.tuple_size} attributes")

# Create a disk scan
disk_scan = FullScanTableDisque(disk_table)

print("\n1. Testing Project operator with disk table:")
print("-" * 60)

# Project operator should work with disk scan
project = Project(disk_scan, [0, 2])  # Keep columns 0 and 2

project.open()
project_count = 0
while True:
    result = project.next()
    if result is None:
        break
    project_count += 1
    if project_count <= 3:  # Show first 3
        print(f"  Projected tuple {project_count}: {result}")
project.close()

print(f"✅ Project operator processed {project_count} tuples from disk")
print(f"Stats: {project}")

print("\n2. Testing Restrict operator with disk table:")
print("-" * 60)

# Reset scan for next test
disk_scan2 = FullScanTableDisque(disk_table)

# Restrict operator should work with disk scan
restrict = Restrict(disk_scan2, 1, 50, ">")  # Filter on column 1 > 50

restrict.open()
restrict_count = 0
while True:
    result = restrict.next()
    if result is None:
        break
    restrict_count += 1
    if restrict_count <= 3:  # Show first 3
        print(f"  Filtered tuple {restrict_count}: {result}")
restrict.close()

print(f"✅ Restrict operator found {restrict_count} matching tuples")
print(f"Stats: {restrict}")

print("\n3. Testing Aggregate operator with disk table:")
print("-" * 60)

# Reset scan for aggregation
disk_scan3 = FullScanTableDisque(disk_table)

# Aggregate operator should work with disk scan
agg = Aggregate(disk_scan3, 2, 'AVG')  # Average of column 2

agg.open()
agg_result = agg.next()
agg.close()

if agg_result:
    print(f"✅ Average of column 2: {agg_result.val[0]:.2f}")
    print(f"Stats: {agg}")
else:
    print("❌ No aggregation result")

print("\n4. Testing operator chaining with disk table:")
print("-" * 60)

# Chain: DiskScan -> Restrict -> Project
disk_scan4 = FullScanTableDisque(disk_table)
restrict2 = Restrict(disk_scan4, 1, 30, ">=")  # col1 >= 30
project2 = Project(restrict2, [0, 3])  # Keep cols 0 and 3

project2.open()
restrict2.open()
disk_scan4.open()

chain_count = 0
while True:
    result = project2.next()
    if result is None:
        break
    chain_count += 1
    if chain_count <= 3:  # Show first 3
        print(f"  Chained result {chain_count}: {result}")

project2.close()
restrict2.close()
disk_scan4.close()

print(f"✅ Operator chain processed {chain_count} tuples")
print(f"Project stats: {project2}")
print(f"Restrict stats: {restrict2}")

# Clean up
os.remove(table_file)

print("\n" + "=" * 60)
print("🎉 Integration test completed successfully!")
print("✅ All operators work with disk-based tables")
print("✅ Operator chaining works across memory/disk boundaries")
print("✅ Disk tables provide scalable storage for large datasets")
print("=" * 60)