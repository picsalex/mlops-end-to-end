import io
from typing import Any

import urllib3
from PIL import Image


def is_annotation_file_valid(json_data: Any) -> bool:
    """
    Validates the structure and content of an annotation file.

    This function checks if the provided JSON data for an annotation file
    contains the required keys ('label', 'bbox', 'image_path') and validates
    the structure and type of values associated with these keys.

    Args:
        json_data (Any): The JSON data from an annotation file.

    Returns:
        bool: True if the annotation file is valid, False otherwise.
    """
    required_keys = {"label", "bbox", "image_path"}
    if not all(key in json_data for key in required_keys):
        return False

    if not json_data["label"] or not isinstance(json_data["label"], list):
        return False

    if not json_data["bbox"] or not isinstance(json_data["bbox"], list):
        return False

    for bbox in json_data["bbox"]:
        if not (
            isinstance(bbox, list)
            and len(bbox) == 4
            and all(isinstance(num, (int, float)) for num in bbox)
        ):
            return False

    return True


def is_image_file_valid(
    image_file_bucket_response: urllib3.response.HTTPResponse,
) -> bool:
    """
    Validates if the provided image file is a valid image.

    This function attempts to open an image file from a given HTTP response
    and verifies it using PIL. If the image is corrupted or cannot be opened,
    the function returns False.

    Args:
        image_file_bucket_response (urllib3.response.HTTPResponse):
            The HTTP response object containing the image data.

    Returns:
        bool: True if the image file is valid, False otherwise.
    """
    try:
        image_data = io.BytesIO(image_file_bucket_response.data)
        with Image.open(image_data) as img:
            img.verify()
        return True
    except Exception:
        return False
