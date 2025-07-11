import datetime

from orso.logging import get_logger
from orso.tools import random_string

from flows.exceptions import FlowError
from flows.exceptions import TimeExceeded


class FlowRunner:
    def __init__(self, flow):
        self.flow = flow
        self.cycles = 0

    def __call__(
        self, data: dict = None, context: dict = None, trace_sample_rate: float = 1 / 1000
    ):
        """
        Create a `run` of a flow and execute with a specific data object.

        Parameters:
            data: dictionary, any (optional)
                The data the flow is to process, opinionated to be a dictionary
                however, any data type is accepted.
            context: dictionary (optional)
                Additional information to support the processing of the data
            trace_sample_rate: float (optional)
                The sample for for to emit trace messages for, default is
                1/1000.
        """
        if not context:
            context = {}

        # create a run_id for the message if it doesn't already have one
        if not context.get("run_id"):
            context["run_id"] = str(random_string(32))

        try:
            # start the flow, walk from the nodes with no incoming links
            for operator_name in self.flow.get_entry_points():
                self._inner_runner(operator_name=operator_name, data=data, context=context)
        except TimeExceeded as te:
            raise te
        except (Exception, SystemExit) as err:
            if hasattr(self, "error_writer"):
                error_log_reference = "NOT LOGGED"
                try:
                    error_payload = (
                        f"timestamp  : {datetime.datetime.today().isoformat()}\n"
                        f"location   : flow_runner\n"
                        f"error type : {type(err).__name__}\n"
                        f"details    : {err}\n"
                        "========================================================================================================================\n"
                        f"{wrap_text('render_error_stack()', 120)}\n"
                        "=======================================================  context  ======================================================\n"
                        f"{wrap_text(str(context), 120)}\n"
                        "========================================================  data  ========================================================\n"
                        f"{wrap_text(str(data), 120)}\n"
                        "========================================================================================================================\n"
                    )
                    error_log_reference = self.error_writer(  # type:ignore
                        error_payload
                    )  # type:ignore
                except:
                    # if we have a uncaught failure, make sure it's logged
                    get_logger().alert(  # type:ignore
                        f"FLOW ABEND - {type(err).__name__} - {err} ({error_log_reference})"
                    )
            raise err

    def _inner_runner(self, operator_name: str = None, data: dict = None, context: dict = None):
        """
        Walk the dag/flow by:
        - Getting the function of the current node
        - Execute the function, wrapped in the base class
        - Find the next step by finding outgoing edges
        - Call this method for the next step
        """
        self.cycles += 1
        if not context:
            context = {}

        operator = self.flow.get_operator(operator_name)
        if operator is None:
            raise FlowError(f"Invalid Flow - Operator {operator_name} is invalid")
        out_going_links = self.flow.get_outgoing_links(operator_name)

        outcome = operator(data, context)

        if outcome:
            if not type(outcome).__name__ in ["generator", "list"]:
                outcome_data, outcome_context = outcome
                outcome = [(outcome_data, outcome_context)]
            for outcome_data, outcome_context in outcome:
                for op_name in out_going_links:
                    self._inner_runner(
                        operator_name=op_name,
                        data=outcome_data,
                        context=outcome_context.copy(),
                    )
