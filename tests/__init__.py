"""Tests for QToolKit."""


from pathlib import Path

module_dir = Path(__file__).resolve().parent
test_dir = module_dir / "test_data"
TEST_DIR = test_dir.resolve()

__all__ = [TEST_DIR]
