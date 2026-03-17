# MiniSGBD Makefile for easy test execution

.PHONY: test all clean

# Run all tests
test: all

# Run all tests with proper Python path
all:
	@echo "🧪 Running all tests..."
	@PYTHONPATH=. python3 tests/TestProject.py
	@echo ""
	@PYTHONPATH=. python3 tests/TestRestrict.py
	@echo ""
	@PYTHONPATH=. python3 tests/TestRestrictChained.py
	@echo ""
	@PYTHONPATH=. python3 tests/TestRestrictChainedFixed.py
	@echo ""
	@echo "🎉 All tests completed!"

# Run specific tests
project:
	@echo "🧪 Running Project tests..."
	@PYTHONPATH=. python3 tests/TestProject.py

restrict:
	@echo "🧪 Running Restrict tests..."
	@PYTHONPATH=. python3 tests/TestRestrict.py

chained:
	@echo "🧪 Running Chained tests..."
	@PYTHONPATH=. python3 tests/TestRestrictChained.py

fixed:
	@echo "🧪 Running Fixed data tests..."
	@PYTHONPATH=. python3 tests/TestRestrictChainedFixed.py

join:
	@echo "🧪 Running Join tests..."
	@PYTHONPATH=. python3 tests/TestJoin.py

# Clean up
clean:
	@echo "🧹 Cleaning up..."
	@rm -rf __pycache__ tests/__pycache__ operators/__pycache__ core/__pycache__
	@echo "✨ Cleaned!"

# Help
help:
	@echo "MiniSGBD Makefile Help:"
	@echo "  make test      - Run all tests"
	@echo "  make all       - Run all tests"
	@echo "  make project   - Run Project tests only"
	@echo "  make restrict  - Run Restrict tests only"
	@echo "  make chained   - Run Chained restrict tests"
	@echo "  make fixed     - Run Fixed data tests"
	@echo "  make join      - Run Join tests only"
	@echo "  make clean     - Clean up cache files"
	@echo "  make help      - Show this help"