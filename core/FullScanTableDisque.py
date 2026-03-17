from typing import Optional
from core.Tuple import Tuple
from core.Instrumentation import Instrumentation
from core.Operateur import Operateur


class FullScanTableDisque(Instrumentation, Operateur):
    """
    Full scan operator for disk-based tables.
    Implements the Operateur interface for disk tables.
    """
    
    def __init__(self, table_disque):
        super().__init__("FullScan" + str(Instrumentation.number))
        Instrumentation.number += 1
        self.table = table_disque
        self.current_tuple = 0
        self.block_cache = {}
        self.reads = 0
        self.total = 0
        self.range = 5
        
    def open(self):
        """Open the table for scanning."""
        self.start()
        self.table.open()
        self.current_tuple = 0
        self.reads = 0
        self.tuplesProduits = 0
        self.memoire = 0
        self.stop()
        
    def next(self) -> Optional[Tuple]:
        """Get next tuple in scan."""
        self.start()
        if self.current_tuple >= self.table.table_size:
            self.stop()
            return None
        
        tuple_obj = self.table.get_tuple(self.current_tuple)
        self.current_tuple += 1
        
        # Count block reads (simplified - would track actual block reads in full implementation)
        if self.current_tuple % self.table.block_size == 0:
            self.reads += 1
        
        if tuple_obj is not None:
            self.produit(tuple_obj)
        
        self.stop()
        return tuple_obj
        
    def close(self):
        """Close the scan."""
        self.start()
        self.total += self.tuplesProduits
        self.table.close()
        self.stop()