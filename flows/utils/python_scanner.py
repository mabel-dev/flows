"""
Python Code Security Scanner

This module provides utilities for scanning Python code to detect potentially unsafe
operations before execution. It uses two layers of protection:

1. AST-based scanning: Lightweight, fast inspection that blocks specific dangerous patterns
2. Bandit integration: More thorough security scanning using the established Bandit tool

Usage:
    from python_scanner import scan_user_code

    try:
        scan_user_code(user_provided_code, severity_threshold="medium")
        # Code is safe, proceed with execution
    except UnsafeCodeError as e:
        # Handle unsafe code detection
        print(f"Unsafe code detected: {e}")

Classes:
    UnsafeCodeError - Exception raised when potentially dangerous code is detected
    UnsafeNodeVisitor - AST visitor that checks for unsafe patterns in Python code

Functions:
    scan_user_code - Main entry point for scanning Python code

Security Features:
    - Blocks dangerous built-in functions like eval(), exec(), compile()
    - Restricts module imports to a safe allowlist
    - Prevents use of system execution functions like os.system()
    - Integrates with Bandit for additional security checks

Dependencies:
    - bandit: Must be installed separately (`pip install bandit`)
"""

import ast
import subprocess  # nosec
import tempfile


class UnsafeCodeError(RuntimeError):
    """Exception raised when potentially unsafe code patterns are detected."""

    pass


class UnsafeNodeVisitor(ast.NodeVisitor):
    """
    AST visitor that traverses Python code to find potentially unsafe patterns.

    Attributes:
        FORBIDDEN_CALLS (set): Built-in function names that are blocked
        SAFE_MODULES (set): Allowlist of modules that can be safely imported
    """

    FORBIDDEN_CALLS = {
        "eval",
        "exec",
        "compile",
        "open",
        "__import__",
        "input",
    }
    SAFE_MODULES = {"json", "math", "decimal", "datetime", "re", "uuid", "orjson"}

    def visit_Import(self, node):
        """
        Check if imported modules are in the safe modules list.

        Args:
            node: AST Import node to check

        Raises:
            UnsafeCodeError: If module is not in the allowlist
        """
        for alias in node.names:
            if alias.name.split(".")[0] not in self.SAFE_MODULES:
                raise UnsafeCodeError(f"Import not allowed: {alias.name}")

    def visit_ImportFrom(self, node):
        """
        Block all 'from X import Y' statements.

        Args:
            node: AST ImportFrom node

        Raises:
            UnsafeCodeError: Always raises, as this import style is blocked
        """
        raise UnsafeCodeError(f"Use of `from ... import ...` is not allowed: {ast.dump(node)}")

    def visit_Call(self, node):
        """
        Check function calls for unsafe operations.

        Blocks direct calls to dangerous built-ins and attribute calls to
        system execution functions.

        Args:
            node: AST Call node to check

        Raises:
            UnsafeCodeError: If unsafe function call is detected
        """
        func = node.func
        if isinstance(func, ast.Name) and func.id in self.FORBIDDEN_CALLS:
            raise UnsafeCodeError(f"Disallowed function call: {func.id}")
        elif isinstance(func, ast.Attribute):
            # e.g. os.system, subprocess.call
            full = self._resolve_attr(func)
            if full in {"os.system", "subprocess.call", "subprocess.Popen"}:
                raise UnsafeCodeError(f"Disallowed attribute call: {full}")
        self.generic_visit(node)

    def _resolve_attr(self, node):
        """
        Resolve an attribute access chain to its full dotted path.

        For example, resolves 'subprocess.call' from the AST nodes.

        Args:
            node: AST node representing an attribute access

        Returns:
            str: Full dotted attribute path (e.g., 'os.system')
        """
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts))


def scan_user_code(code: str, severity_threshold: str = "low") -> None:
    """
    Scans Python code for security issues using AST analysis and Bandit.

    This function performs a two-stage security scan:
    1. AST-based scanning to block obviously dangerous patterns
    2. Bandit security scanner for more thorough analysis

    Parameters:
        code: str
            The Python source code to analyze.
        severity_threshold: str
            The minimum Bandit severity level to reject ("low", "medium", or "high").

    Raises:
        UnsafeCodeError: If AST scan detects unsafe patterns
        RuntimeError: If Bandit detects issues at or above the specified severity

    Note:
        Requires the bandit package to be installed.
    """
    # AST-based scanning (cheap and hard-blocking)
    try:
        tree = ast.parse(code)
        UnsafeNodeVisitor().visit(tree)
    except SyntaxError as e:
        raise UnsafeCodeError(f"Syntax error in code: {e}")
    except UnsafeCodeError as e:
        raise

    # Bandit scan (heavier, best effort)
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(code)
        f.flush()
        script_path = f.name

    allowed_severities = {"low", "medium", "high"}
    if severity_threshold not in allowed_severities:
        raise ValueError(f"Invalid severity_threshold: {severity_threshold}")

    result = subprocess.run(
        ["bandit", "-q", "-r", script_path, "--severity-level", severity_threshold],
        capture_output=True,
        text=True,
        shell=False,
    )  # nosec

    if result.returncode != 0:
        raise RuntimeError(f"Bandit found potential issues:\n{result.stdout.strip()}")
