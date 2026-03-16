import re


def curate_value(value, owncloud_images_token: str):
    # Check if value matches the IMG_YYYYMMDD_HHMMSS.jpg pattern
    if isinstance(value, str) and re.match(r"^IMG_\d{8}_\d{6}\.jpg$", value):
        return f"https://oc.embl.de/index.php/s/{owncloud_images_token}/download?path=%2F&files={value}"

    # If number is of the form ".8", normalise to "0.8"
    if isinstance(value, str) and re.match(r"^\.\d+$", value):
        return f"0{value}"

    return value
