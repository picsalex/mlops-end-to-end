from typing import Optional

from zenml.client import Client


def get_tracker_name() -> Optional[str]:
    """Get the name of the active experiment tracker."""

    experiment_tracker = Client().active_stack.experiment_tracker
    return experiment_tracker.name if experiment_tracker else None
