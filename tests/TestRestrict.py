from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Restrict import Restrict

# 1. Création de la table (taille tuple=3, range=100, taille table=5)
# Note: randomize(tuplesize, val_range, tablesize)
table = TableMemoire.randomize(3, 100, 5)

print("Table générée (Data):")
for t in table.valeurs:
    print(t)
print("-" * 30)

# 2. Scan
scan = FullScanTableMemoire(table)

# 3. Restriction (garder seulement les tuples où col 1 == 42)
# SELECT * FROM table WHERE col1 == 42
restrict = Restrict(scan, 1, 42, "==")

# 4. Exécution
print("Résultat de la restriction (col 1 == 42):")
restrict.open()
while True:
    t = restrict.next()
    if t is None:
        break
    print(t)
restrict.close()

# 5. Stats
print("-" * 30)
print(restrict)