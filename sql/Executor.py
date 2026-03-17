"""
Executor – drives an operator tree and collects results.
"""

from __future__ import annotations

from core.Operateur import Operateur
from core.Tuple import Tuple


class Executor:
    """
    Stateless helper that executes an operator tree.

    All methods are static – no instance needed.
    """

    @staticmethod
    def execute(op: Operateur) -> list[list]:
        """
        Run *op* and return all result tuples as a list of plain Python lists.
        """
        results: list[list] = []
        op.open()
        while True:
            t: Tuple | None = op.next()
            if t is None:
                break
            results.append(list(t.val))
        op.close()
        return results

    @staticmethod
    def execute_and_print(
        op:      Operateur,
        headers: list[str] | None = None,
    ) -> int:
        """
        Run *op*, print each row, and return the total row count.

        Parameters
        ----------
        op:
            Root operator of the execution tree.
        headers:
            Optional column header labels.  When supplied, a header line
            and separator are printed before the rows.
        """
        if headers:
            print("\t".join(str(h) for h in headers))
            print("-" * max(40, 8 * len(headers)))

        op.open()
        count = 0
        while True:
            t = op.next()
            if t is None:
                break
            print(t)
            count += 1
        op.close()

        print(f"({count} row{'s' if count != 1 else ''})")
        return count
