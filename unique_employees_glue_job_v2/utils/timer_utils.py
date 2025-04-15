import datetime

def get_current_timestamp(fmt: str = "%Y-%m-%d_%H-%M-%S") -> str:
    """Returns the current timestamp formatted as a string."""
    return datetime.datetime.now().strftime(fmt)