import json
import subprocess  # nosec
import tempfile
from typing import IO
from typing import Generator
from typing import Optional

from flows.internal.base import BaseOperator
from flows.internal.python.python_scanner import scan_user_code


class PythonStep(BaseOperator):
    """
    A sandboxed step that executes user-provided Python in a persistent subprocess.
    """

    def __init__(self, config: dict, flow_config: dict):
        self.config = config
        self.flow_config = flow_config
        self._proc: Optional[subprocess.Popen] = None
        self._stdin: Optional[IO] = None
        self._stdout: Optional[IO] = None

        self._start_subprocess()

    def _start_subprocess(self):
        code = self.config.get("code")
        if not code:
            raise ValueError("Missing `code` in config")

        scan_user_code(code)

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(code)
            f.flush()
            script_path = f.name

        cmd = ["python", "-u", "sandbox/runner.py", script_path]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered
            shell=False,  # nosec
        )
        self._stdin = self._proc.stdin
        self._stdout = self._proc.stdout

    def execute(self, data: Optional[dict] = None, context: dict = None) -> Generator:
        """
        Sends one row to the subprocess, receives one transformed row back.
        """
        if self._proc is None or self._stdin is None or self._stdout is None:
            raise RuntimeError("Subprocess not started")

        payload = {"data": data, "context": context, "flow_config": self.flow_config}
        self._stdin.write(json.dumps(payload) + "\n")
        self._stdin.flush()

        line = self._stdout.readline()
        if not line:
            raise RuntimeError("No response from sandbox")

        response = json.loads(line)
        yield response["data"], response["context"]

    def close(self):
        if self._proc:
            self._stdin.close()
            self._proc.terminate()
            self._proc.wait(timeout=2)
