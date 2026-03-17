"""
Unit tests for Project operator using pytest.
"""

import pytest
from core.TableMemoire import TableMemoire
from core.FullScanTableMemoire import FullScanTableMemoire
from operators.Project import Project


class TestProjectOperator:
    """Test Project operator functionality"""
    
    def test_project_basic(self):
        """Test basic projection"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [0, 2])  # Keep columns 0 and 2
        
        project.open()
        count = 0
        while project.next() is not None:
            count += 1
        project.close()
        
        assert count == 5  # Should have same number of tuples
        assert project.tuplesProduits == 5
    
    def test_project_empty(self):
        """Test projection on empty table"""
        table = TableMemoire(3)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [0, 1])
        
        project.open()
        result = project.next()
        project.close()
        
        assert result is None  # Should return None for empty table
        assert project.tuplesProduits == 0
    
    def test_project_single_column(self):
        """Test projection with single column"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [1])  # Keep only column 1
        
        project.open()
        result = project.next()
        project.close()
        
        assert result is not None
        assert len(result.val) == 1  # Should have only 1 column
        assert project.tuplesProduits == 1  # Only called next() once
    
    def test_project_all_columns(self):
        """Test projection with all columns"""
        table = TableMemoire.randomize(3, 100, 5)
        scan = FullScanTableMemoire(table)
        project = Project(scan, [0, 1, 2])  # Keep all columns
        
        project.open()
        result = project.next()
        project.close()
        
        assert result is not None
        assert len(result.val) == 3  # Should have all 3 columns
        assert project.tuplesProduits == 1  # Only called next() once


if __name__ == "__main__":
    pytest.main([__file__, "-v"])