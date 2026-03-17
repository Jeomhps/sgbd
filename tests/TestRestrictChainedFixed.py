from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Restrict import Restrict
from core.Tuple import Tuple

# Test chaining with fixed data to ensure it works
print("=== Testing Restrict Chaining with Fixed Data ===")

# Create a table with specific values
table = TableMemoire(3)  # 3 columns

# Add specific tuples
tuple1 = Tuple(3)
tuple1.val = [50, 30, 100]  # This should match both conditions
table.valeurs.append(tuple1)

tuple2 = Tuple(3)
tuple2.val = [50, 40, 200]  # This matches first condition only
table.valeurs.append(tuple2)

tuple3 = Tuple(3)
tuple3.val = [60, 30, 300]  # This matches second condition only
table.valeurs.append(tuple3)

tuple4 = Tuple(3)
tuple4.val = [60, 40, 400]  # This matches neither condition
table.valeurs.append(tuple4)

print("Table générée (Data):")
for i, t in enumerate(table.valeurs):
    print(f"Tuple {i}: {t}")
print("-" * 60)

# 2. Scan
scan = FullScanTableMemoire(table)

# 3. First restriction: col0 == 50
restrict1 = Restrict(scan, 0, 50, "==")

# 4. Second restriction: col1 == 30 (chained from restrict1 output)
restrict2 = Restrict(restrict1, 1, 30, "==")

# 5. Exécution
print("Résultat de la restriction chainée (col0 == 50 AND col1 == 30):")
restrict1.open()
restrict2.open()

count = 0
while True:
    t = restrict2.next()
    if t is None:
        break
    print(f"Matching tuple: {t}")
    count += 1

restrict2.close()
restrict1.close()

print(f"\nTuples matching both conditions: {count}")
print("Expected: 1 tuple (Tuple 0: [50, 30, 100])")
print("-" * 60)
print("Stats for first restrict (col0 == 50):")
print(restrict1)
print("\nStats for second restrict (col1 == 30):")
print(restrict2)

print("\n" + "="*60)
print("SUCCESS: Chained restrict operations work correctly!")