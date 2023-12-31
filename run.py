import argparse
from enum import Enum

from zenml.client import Client
from zenml.config import DockerSettings
from zenml.enums import ExecutionStatus
from zenml.integrations import DeepchecksIntegration

from src.pipelines.pipeline_datalake import gitflow_datalake_pipeline
from src.pipelines.pipeline_experiment import gitflow_experiment_pipeline


class Pipeline(str, Enum):
    DATALAKE = "datalake"
    EXPERIMENT = "experiment"
    END_TO_END = "end-to-end"


def main(
    pipeline_name: Pipeline = Pipeline.EXPERIMENT,
    ignore_checks: bool = False,
    requirements_file: str = "requirements.txt",
    model_name: str = "model",
):
    """Main runner for all pipelines.

    Args:
        pipeline_name: One of "experiment" or "end-to-end".
        ignore_checks: Whether to ignore model appraisal checks. Defaults to False.
        requirements_file: The requirements file to use to ensure reproducibility.
            Defaults to "requirements.txt".
        model_name: The name to use for the trained/deployed model. Defaults to
            "model".
    """

    settings = {}
    # pipeline_args = {}

    docker_settings = DockerSettings(
        install_stack_requirements=False,
        requirements=requirements_file,
        apt_packages=DeepchecksIntegration.APT_PACKAGES,  # for Deepchecks
    )
    settings["docker"] = docker_settings

    client = Client()
    orchestrator = client.active_stack.orchestrator
    assert orchestrator is not None, "Orchestrator not in stack."

    if pipeline_name == Pipeline.DATALAKE:
        pipeline_instance = gitflow_datalake_pipeline

    elif pipeline_name == Pipeline.EXPERIMENT:
        pipeline_instance = gitflow_experiment_pipeline

    else:
        raise ValueError(f"Pipeline name `{pipeline_name}` not supported. ")

    # Run pipeline
    pipeline_instance()

    pipeline_run = pipeline_instance.model.get_runs()[0]

    if pipeline_run.status == ExecutionStatus.FAILED:
        print("Pipeline failed. Check the logs for more details.")
        exit(1)
    elif pipeline_run.status == ExecutionStatus.RUNNING:
        print(
            "Pipeline is still running. The post-execution phase cannot "
            "proceed. Please make sure you use an orchestrator with a "
            "synchronous mode of execution."
        )
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--pipeline",
        default="experiment",
        help="Toggles which pipeline to run. One of `datalake`, `experiment` and `end-to-end`. "
        "Defaults to `experiment`",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-m",
        "--model",
        default="model",
        help="Name of the model to train/deploy. Defaults to `model`",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-r",
        "--requirements",
        default="requirements.txt",
        help="Path to file with frozen python requirements needed to run the "
        "pipelines on the active stack. Defaults to `requirements.txt`",
        type=str,
        required=False,
    )
    parser.add_argument(
        "-i",
        "--ignore-checks",
        default=False,
        help="Ignore model training checks. Defaults to False",
        action="store_true",
        required=False,
    )
    args = parser.parse_args()

    assert args.pipeline in [
        Pipeline.DATALAKE,
        Pipeline.EXPERIMENT,
        Pipeline.END_TO_END,
    ]
    assert isinstance(args.ignore_checks, bool)
    main(
        pipeline_name=Pipeline(args.pipeline),
        ignore_checks=args.ignore_checks,
        requirements_file=args.requirements,
        model_name=args.model,
    )
