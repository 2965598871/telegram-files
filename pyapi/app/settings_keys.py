DEFAULT_SETTINGS: dict[str, object | None] = {
    "version": None,
    "uniqueOnly": False,
    "imageLoadSize": None,
    "alwaysHide": False,
    "showSensitiveContent": False,
    "automation": None,
    "autoDownloadLimit": None,
    "autoDownloadTimeLimited": None,
    "proxys": None,
    "avgSpeedInterval": 5 * 60,
    "speedUnits": "bits",
    "tags": None,
}


def default_value_for(key: str) -> object | None:
    if key not in DEFAULT_SETTINGS:
        raise KeyError(f"Unknown setting key: {key}")
    return DEFAULT_SETTINGS[key]
