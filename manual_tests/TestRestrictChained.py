from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Restrict import Restrict

# Test chaining multiple restrict operations
# SQL equivalent: SELECT * FROM table WHERE col0 == 50 AND col1 == 30

# 1. Création de la table (taille tuple=3, range=100, taille table=10)
table = TableMemoire.randomize(3, 100, 10)

print("Table générée (Data):")
for i, t in enumerate(table.valeurs):
    print(f"Tuple {i}: {t}")
print("-" * 50)

# 2. Scan
scan = FullScanTableMemoire(table)

# 3. First restriction: col0 == 50
# SELECT * FROM table WHERE col0 == 50
restrict1 = Restrict(scan, 0, 50, "==")

# 4. Second restriction: col1 == 30 (chained from restrict1 output)
# SELECT * FROM table WHERE col0 == 50 AND col1 == 30
restrict2 = Restrict(restrict1, 1, 30, "==")

# 5. Exécution
print("Résultat de la restriction chainée (col0 == 50 AND col1 == 30):")
restrict1.open()  # Open the first restrict
restrict2.open()  # Open the second restrict

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
print("-" * 50)
print("Stats for first restrict (col0 == 50):")
print(restrict1)
print("\nStats for second restrict (col1 == 30):")
print(restrict2)