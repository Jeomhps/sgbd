from core.Instrumentation import Instrumentation
from core.Operateur import Operateur
from core.Tuple import Tuple
from typing import Optional

class Aggregate(Instrumentation, Operateur):
    """
    Agregation : AVG, SUM, MIN, MAX, COUNT.

    Sans group_by : retourne un seul tuple avec le resultat.
    Avec group_by : retourne un tuple (cle_groupe, resultat) par groupe.
    """

    VALID_FUNCS = ('AVG', 'SUM', 'MIN', 'MAX', 'COUNT')

    def __init__(self, _in, _agg_col, _agg_func, _group_cols=None):
        super().__init__("Aggregate" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.child = _in
        self.agg_col = _agg_col
        self.agg_func = _agg_func.upper()
        self.group_cols = _group_cols  # liste d'indices de colonnes de regroupement

        if self.agg_func not in self.VALID_FUNCS:
            raise ValueError(f"Fonction inconnue: {self.agg_func}. Valides: {self.VALID_FUNCS}")

        self._results = []
        self._result_index = 0

    def open(self):
        self.start()
        self.child.open()
        self._results = []
        self._result_index = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()

    def next(self) -> Optional[Tuple]:
        self.start()

        # Collecter et calculer au premier appel
        if not self._results:
            self._compute()

        if self._result_index >= len(self._results):
            self.stop()
            return None

        t = self._results[self._result_index]
        self._result_index += 1
        self.produit(t)
        self.stop()
        return t

    def _compute(self):
        """Collecte les tuples et calcule le(s) resultat(s)."""
        # Collecter tous les tuples
        all_tuples = []
        while True:
            t = self.child.next()
            if t is None:
                break
            all_tuples.append(t)

        if self.group_cols:
            # Regrouper par cles
            groups = {}
            for t in all_tuples:
                key = tuple(t.val[c] for c in self.group_cols)
                if key not in groups:
                    groups[key] = []
                groups[key].append(t.val[self.agg_col])

            for key, values in groups.items():
                result_val = self._aggregate(values)
                result = Tuple(len(key) + 1)
                for i, k in enumerate(key):
                    result.val[i] = k
                result.val[len(key)] = result_val
                self._results.append(result)
        else:
            # Pas de regroupement : un seul resultat
            values = [t.val[self.agg_col] for t in all_tuples]
            result = Tuple(1)
            result.val[0] = self._aggregate(values)
            self._results.append(result)

    def _aggregate(self, values):
        """Calcule le resultat de l'agregation sur une liste de valeurs."""
        if not values:
            return 0
        if self.agg_func == 'COUNT': return len(values)
        if self.agg_func == 'SUM':   return sum(values)
        if self.agg_func == 'AVG':   return sum(values) / len(values)
        if self.agg_func == 'MIN':   return min(values)
        if self.agg_func == 'MAX':   return max(values)
        return 0

    def close(self):
        self.child.close()
        self._results = []
        self._result_index = 0
