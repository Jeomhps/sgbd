"""
Unit tests for Tuple class.
"""

import pytest
from core.Tuple import Tuple


class TestTuple:
    """Test Tuple functionality"""
    
    def test_tuple_creation(self):
        """Test basic tuple creation"""
        tuple_obj = Tuple(3)
        assert tuple_obj.size == 3
        assert len(tuple_obj.val) == 3
        assert all(val == 0 for val in tuple_obj.val)  # Default initialization
    
    def test_tuple_with_values(self):
        """Test tuple with specific values"""
        tuple_obj = Tuple(2)
        tuple_obj.val = [42, 100]
        assert tuple_obj.val == [42, 100]
        assert tuple_obj.size == 2
    
    def test_tuple_string_representation(self):
        """Test tuple string representation"""
        tuple_obj = Tuple(3)
        tuple_obj.val = [1, 2, 3]
        # Should have a reasonable string representation
        assert str(tuple_obj) is not None
        assert "1" in str(tuple_obj)
        assert "2" in str(tuple_obj)
        assert "3" in str(tuple_obj)
    
    def test_tuple_equality(self):
        """Test tuple equality comparison"""
        t1 = Tuple(2)
        t1.val = [10, 20]
        
        t2 = Tuple(2)
        t2.val = [10, 20]
        
        t3 = Tuple(2)
        t3.val = [30, 40]
        
        # Different objects with same values should be equal
        assert t1.val == t2.val
        # Different values should not be equal
        assert t1.val != t3.val
    
    def test_tuple_different_sizes(self):
        """Test tuples with different attribute counts"""
        t1 = Tuple(1)
        t2 = Tuple(5)
        t3 = Tuple(10)
        
        assert t1.size == 1
        assert t2.size == 5
        assert t3.size == 10
        
        assert len(t1.val) == 1
        assert len(t2.val) == 5
        assert len(t3.val) == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])