from sklearn.base import ClassifierMixin
from ultralytics import YOLO
from zenml import step

from src.models.model_dataset import Dataset
from src.utils.tracker_helper import get_tracker_name


@step(
    enable_cache=False,
    experiment_tracker=get_tracker_name(),
)
def model_trainer(dataset: Dataset) -> ClassifierMixin:
    model = YOLO()
    model.train()
