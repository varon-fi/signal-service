"""Compatibility re-exports for shared strategy interfaces.

This module keeps backwards compatibility while deferring to varon_fi.
"""

from varon_fi import BaseStrategy, Signal

__all__ = ["BaseStrategy", "Signal"]
