import time
from core.TableMemoire import TableMemoire
from sql import Catalog, SQLParser, QueryPlanner, Executor

cat = Catalog()
cat.register("T1", TableMemoire.randomize(3, 100, 20))
cat.register("T2", TableMemoire.randomize(3, 100, 20))

sql = "SELECT * FROM T1, T2 WHERE T1.A1 = T2.A1 AND T1.A2 > 50"

t0 = time.perf_counter()
op, plan = QueryPlanner(cat).plan(SQLParser(sql).parse())
t1 = time.perf_counter()

print(plan)
print()

t2 = time.perf_counter()
Executor.execute_and_print(op)
t3 = time.perf_counter()

print()
print(f"Planification : {(t1 - t0) * 1000:.3f} ms")
print(f"Exécution     : {(t3 - t2) * 1000:.3f} ms")
print(f"Total         : {(t3 - t0) * 1000:.3f} ms")
