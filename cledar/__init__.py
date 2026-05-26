"""Namespace package definition for ``cledar``.

This allows multiple distributions (e.g. ``cledar.*``) to share the
same top-level package without shadowing each other.
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
