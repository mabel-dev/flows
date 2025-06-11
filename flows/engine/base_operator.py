"""
Base Operator Module

This module defines the BaseOperator class, which provides a common structure,
interfaces, and utility methods for all Operator classes in the pipeline.
It manages execution, retries, error handling, logging, and versioning for
operators, allowing engineers to focus on implementing data processing logic.
"""

import datetime
import functools
import hashlib
import inspect
import re
import sys
import time
from typing import Generator
from typing import Tuple

from orso.logging import get_logger  # type:ignore
from orso.tools import random_string

SIGTERM = random_string(64)


class BaseOperator:
    """
    Base class for all Operators in the pipeline.

    Provides common functionality such as retry logic, error handling,
    logging, execution time tracking, and versioning. Subclasses should
    override the `execute` method to implement their specific logic.
    """

    sigterm = SIGTERM  # default signal to use for graceful shutdown

    def __init__(self, **kwargs):
        """
        Initialize the BaseOperator with optional configuration parameters.

        Parameters:
            retry_count (int, optional): Number of retry attempts (default: 2, range: 1-5).
            retry_wait (int, optional): Seconds to wait between retries (default: 5, range: 1-300).
            rolling_failure_window (int, optional): Number of previous executions to track for failures (default: 10, range: 1-100).
        """
        self.flow = None
        self.records_processed = 0  # number of times this Operator has been run
        self.execution_time_ns = 0  # nano seconds of cpu execution time
        self.errors = 0  # number of errors
        self.commencement_time = None  # the time processing started
        self.logger = get_logger()  # get the mabel logger

        self.name = self.__class__.__name__

        # read retry settings, clamp values to practical ranges
        self.retry_count = self._clamp(kwargs.get("retry_count", 2), 1, 5)
        self.retry_wait = self._clamp(kwargs.get("retry_wait", 5), 1, 300)
        rolling_failure_window = self._clamp(kwargs.get("rolling_failure_window", 10), 1, 100)
        self.last_few_results = [1] * rolling_failure_window  # track the last n results

        # Log the hashes of the __call__ and version methods
        call_hash = self._hash(inspect.getsource(self.__call__))[-12:]
        version_hash = self._hash(inspect.getsource(self.version))[-12:]
        self.logger.audit(
            {
                "operator": self.name,
                "call_hash": call_hash,
                "version_hash": self.version(),
                "version_method": version_hash,
            }
        )

        self.config = kwargs

    def execute(
        self, data: dict = None, context: dict = None
    ) -> Generator[Tuple[dict, dict], None, None]:
        """
        YOU MUST OVERRIDE THIS METHOD

        This is where the main logic for the Operator is implemented.

        Parameters:
            data: Dictionary (or Any)
                The data to be processed, the Base Operator is opinionated
                for the data to be a dictionary, but any data type will work
            context: Dictionary
                Information to support the exeuction of the Operator

        Returns:
            None
                Do not continue further through the flow
            Tuple (data, context)
                The data to pass to the next Operator
            Iterable(data, context)
                Run the next Operator multiple times
        """
        pass  # pragma: no cover

    def __call__(self, data: dict = None, context: dict = None):
        """
        DO NOT OVERRIDE THIS METHOD

        This method wraps the `execute` method, which must be overridden, to
        to add management of the execution such as sensors and retries.
        """
        if self.commencement_time is None:
            self.commencement_time = datetime.datetime.now()
        self.records_processed += 1
        attempts_to_go = self.retry_count
        while attempts_to_go > 0:
            try:
                start_time = time.perf_counter_ns()
                outcome = self.execute(data, context)
                my_execution_time = time.perf_counter_ns() - start_time
                self.execution_time_ns += my_execution_time
                # add a success to the last_few_results list
                self.last_few_results.append(1)
                self.last_few_results.pop(0)
                break
            except Exception as err:
                self.errors += 1
                attempts_to_go -= 1
                if attempts_to_go:
                    self.logger.error(
                        f"{self.name} - {type(err).__name__} - {err} - retry in {self.retry_wait} seconds ({context.get('uuid')})"
                    )
                    time.sleep(self.retry_wait)
                else:
                    error_log_reference = ""
                    error_reference = err
                    try:
                        error_payload = (
                            f"timestamp  : {datetime.datetime.today().isoformat()}\n"
                            f"operator   : {self.name}\n"
                            f"error type : {type(err).__name__}\n"
                            f"details    : {err}\n"
                            "------------------------------------------------------------------------------------------------------------------------\n"
                            f"{self._wrap_text('RenderErrorStack()', 120)}\n"
                            "-------------------------------------------------------  context  ------------------------------------------------------\n"
                            f"{self._wrap_text(str(context), 120)}\n"
                            "--------------------------------------------------------  data  --------------------------------------------------------\n"
                            f"{self._wrap_text(str(data), 120)}\n"
                            "------------------------------------------------------------------------------------------------------------------------\n"
                        )
                        error_log_reference = self.error_writer(error_payload)  # type:ignore
                    except Exception as err:
                        self.logger.error(
                            f"Problem writing to the error bin, a record has been lost. {type(err).__name__} - {err} - {context.get('uuid')}"
                        )
                    finally:
                        # finally blocks are called following a try/except block regardless of the outcome
                        self.logger.alert(
                            f"{self.name} - {type(error_reference).__name__} - {error_reference} - tried {self.retry_count} times before aborting ({context.get('uuid')}) {error_log_reference}"
                        )
                    outcome = None
                    # add a failure to the last_few_results list
                    self.last_few_results.append(0)
                    self.last_few_results.pop(0)

        # if there is a high failure rate, abort
        if sum(self.last_few_results) < (len(self.last_few_results) / 2):
            self.logger.alert(
                f"Failure Rate for {self.name} over last {len(self.last_few_results)} executions is over 50%, aborting."
            )
            sys.exit(1)

        return outcome

    def read_sensors(self):
        """
        Format data about the transformation, this can be overridden but it
        should include this information
        """
        response = {
            "operator": self.name,
            "version": self.version(),
            "records_processed": self.records_processed,
            "error_count": self.errors,
            "execution_sec": self.execution_time_ns / 1e9,
        }
        if self.records_processed == 0:
            self.logger.warning(f"{self.name} processed 0 records")
        if self.commencement_time:
            response["commencement_time"] = self.commencement_time.isoformat()
        return response

    @functools.lru_cache(1)
    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the Operator code, this is intended to facilitate
        reproducability and auditability of the pipeline. The version is the
        last 16 characters of the hash of the source code of the 'execute'
        method. This removes the need for the developer to remember to
        increment a version variable.

        Hashing isn't security sensitive here, it's to identify changes
        rather than protect information.
        """
        source = inspect.getsource(self.execute)
        source = self._only_alpha_nums(source)
        full_hash = hashlib.sha256(source.encode())
        return full_hash.hexdigest()[-16:]

    def __del__(self):
        # do nothing - prevents errors if someone thinks they're being a good
        # citizen and calls super().__del__
        pass

    def _clamp(self, value, low_bound, high_bound):
        """
        'clamping' is fixing a value within a range
        """
        value = min(value, high_bound)
        value = max(value, low_bound)
        return value

    def _only_alpha_nums(self, text):
        """
        Remove all non-alphanumeric characters from a string.
        """
        pattern = re.compile(r"[\W_]+")
        return pattern.sub("", text)

    def _hash(self, block):
        """
        Compute the SHA-256 hash of a given input.
        """
        bytes_object = str(block)
        raw_hash = hashlib.sha256(bytes_object.encode())
        hex_hash = raw_hash.hexdigest()
        return hex_hash

    def _wrap_text(self, text, line_len):
        """
        Wrap text to a specified line length.
        """
        from textwrap import fill

        def _inner(text):
            for line in text.splitlines():
                yield fill(line, line_len)

        return "\n".join(list(_inner(text)))
