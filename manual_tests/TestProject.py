from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Project import Project

# 1. Création de la table (taille tuple=3, range=100, taille table=5)
# Note: randomize(tuplesize, val_range, tablesize)
table = TableMemoire.randomize(3, 100, 5)

print("Table générée (Data):")
for t in table.valeurs:
    print(t)
print("-" * 30)

# 2. Scan
scan = FullScanTableMemoire(table)

# 3. Projection (garder col 0 et 2)
# SELECT col0, col2 FROM table
project = Project(scan, [0, 2])

# 4. Exécution
print("Résultat de la projection (col 0 et 2):")
project.open()
while True:
    t = project.next()
    if t is None:
        break
    print(t)
project.close()

# 5. Stats
print("-" * 30)
print(project)
