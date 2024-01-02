from sklearn.base import ClassifierMixin
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from zenml import step

from src.models.model_dataset import Dataset
from src.utils.tracker_helper import get_tracker_name, log_model


@step(
    enable_cache=False,
    experiment_tracker=get_tracker_name(),
)
def decision_tree_trainer(dataset: Dataset) -> ClassifierMixin:
    """Train a sklearn decision tree classifier.

    If the experiment tracker is enabled, the model and the training accuracy
    will be logged to the experiment tracker.

    Args:
        params: The hyperparameters for the model.
        train_dataset: The training dataset to train the model on.

    Returns:
        The trained model and the training accuracy.
    """
    # enable_autolog()
    iris = load_iris()
    X, y = iris.data, iris.target
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=123
    )

    model = DecisionTreeClassifier(
        max_depth=5,
    )

    model.fit(X_train, y_train)
    train_acc = model.score(X_test, y_test)
    print(f"Train accuracy: {train_acc}")
    log_model(model, "model")
    return model
