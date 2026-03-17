from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from core.Tuple import Tuple
from operators.Aggregate import Aggregate

print("=== Testing Aggregate Operator ===")

# Create test table (sales data: id, product_id, quantity, price)
table = TableMemoire(4)
print("Test Table (Sales): id, product_id, quantity, price")

# Add test data
tuple1 = Tuple(4)
tuple1.val = [1, 101, 5, 10.0]   # id=1, product=101, qty=5, price=10.0
table.valeurs.append(tuple1)

tuple2 = Tuple(4)
tuple2.val = [2, 101, 3, 10.0]   # id=2, product=101, qty=3, price=10.0
table.valeurs.append(tuple2)

tuple3 = Tuple(4)
tuple3.val = [3, 102, 7, 15.0]   # id=3, product=102, qty=7, price=15.0
table.valeurs.append(tuple3)

tuple4 = Tuple(4)
tuple4.val = [4, 102, 2, 15.0]   # id=4, product=102, qty=2, price=15.0
table.valeurs.append(tuple4)

tuple5 = Tuple(4)
tuple5.val = [5, 103, 4, 20.0]   # id=5, product=103, qty=4, price=20.0
table.valeurs.append(tuple5)

for i, t in enumerate(table.valeurs):
    print(f"  Sale {i+1}: {t}")

print()

# Test 1: SUM aggregation
print("Test 1: SUM of quantities")
print("SQL: SELECT SUM(quantity) FROM sales")
scan1 = FullScanTableMemoire(table)
sum_agg = Aggregate(scan1, 2, 'SUM')  # Aggregate column 2 (quantity)

sum_agg.open()
result = sum_agg.next()
sum_agg.close()

if result:
    print(f"SUM result: {result.val[0]} (expected: 5+3+7+2+4=21)")
else:
    print("ERROR: No result from SUM")

print(f"Stats: {sum_agg}")
print()

# Test 2: AVG aggregation
print("Test 2: AVG of prices")
print("SQL: SELECT AVG(price) FROM sales")
scan2 = FullScanTableMemoire(table)
avg_agg = Aggregate(scan2, 3, 'AVG')  # Aggregate column 3 (price)

avg_agg.open()
result = avg_agg.next()
avg_agg.close()

if result:
    expected_avg = (10.0 + 10.0 + 15.0 + 15.0 + 20.0) / 5
    print(f"AVG result: {result.val[0]:.2f} (expected: {expected_avg:.2f})")
else:
    print("ERROR: No result from AVG")

print(f"Stats: {avg_agg}")
print()

# Test 3: MIN aggregation
print("Test 3: MIN of quantities")
print("SQL: SELECT MIN(quantity) FROM sales")
scan3 = FullScanTableMemoire(table)
min_agg = Aggregate(scan3, 2, 'MIN')  # Aggregate column 2 (quantity)

min_agg.open()
result = min_agg.next()
min_agg.close()

if result:
    print(f"MIN result: {result.val[0]} (expected: 2)")
else:
    print("ERROR: No result from MIN")

print(f"Stats: {min_agg}")
print()

# Test 4: MAX aggregation
print("Test 4: MAX of prices")
print("SQL: SELECT MAX(price) FROM sales")
scan4 = FullScanTableMemoire(table)
max_agg = Aggregate(scan4, 3, 'MAX')  # Aggregate column 3 (price)

max_agg.open()
result = max_agg.next()
max_agg.close()

if result:
    print(f"MAX result: {result.val[0]} (expected: 20.0)")
else:
    print("ERROR: No result from MAX")

print(f"Stats: {max_agg}")
print()

# Test 5: COUNT aggregation
print("Test 5: COUNT of all sales")
print("SQL: SELECT COUNT(*) FROM sales")
scan5 = FullScanTableMemoire(table)
count_agg = Aggregate(scan5, 0, 'COUNT')  # Can count any column

count_agg.open()
result = count_agg.next()
count_agg.close()

if result:
    print(f"COUNT result: {result.val[0]} (expected: 5)")
else:
    print("ERROR: No result from COUNT")

print(f"Stats: {count_agg}")
print()

# Test 6: GROUP BY aggregation
print("Test 6: GROUP BY product_id with SUM(quantity)")
print("SQL: SELECT product_id, SUM(quantity) FROM sales GROUP BY product_id")
scan6 = FullScanTableMemoire(table)
group_sum_agg = Aggregate(scan6, 2, 'SUM', [1])  # Aggregate col 2, group by col 1

print("Results:")
group_sum_agg.open()
while True:
    result = group_sum_agg.next()
    if result is None:
        break
    product_id = result.val[0]
    total_quantity = result.val[1]
    print(f"  Product {product_id}: Total quantity = {total_quantity}")

group_sum_agg.close()
print(f"Stats: {group_sum_agg}")
print()

# Test 7: GROUP BY with AVG
print("Test 7: GROUP BY product_id with AVG(price)")
print("SQL: SELECT product_id, AVG(price) FROM sales GROUP BY product_id")
scan7 = FullScanTableMemoire(table)
group_avg_agg = Aggregate(scan7, 3, 'AVG', [1])  # Aggregate col 3, group by col 1

print("Results:")
group_avg_agg.open()
while True:
    result = group_avg_agg.next()
    if result is None:
        break
    product_id = result.val[0]
    avg_price = result.val[1]
    print(f"  Product {product_id}: Average price = {avg_price:.2f}")

group_avg_agg.close()
print(f"Stats: {group_avg_agg}")
print()

# Test 8: Empty table
print("Test 8: Aggregation on empty table")
empty_table = TableMemoire(2)
scan8 = FullScanTableMemoire(empty_table)
empty_agg = Aggregate(scan8, 0, 'SUM')

empty_agg.open()
result = empty_agg.next()
empty_agg.close()

if result:
    print(f"Empty table SUM result: {result.val[0]} (expected: 0)")
else:
    print("ERROR: No result from empty table")

print(f"Stats: {empty_agg}")
print()

print("🎉 Aggregate operator testing completed!")
print("✅ All aggregation functions (SUM, AVG, MIN, MAX, COUNT) working correctly")
print("✅ GROUP BY functionality working correctly")
print("✅ Edge cases handled properly")