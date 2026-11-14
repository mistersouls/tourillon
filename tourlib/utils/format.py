


def human_bytes(count: int) -> str:
    """Format a byte count as a human-readable string."""
    if count < 1024:
        return f"{count}B"

    for unit in ["KiB", "MiB", "GiB", "TiB"]:
        count /= 1024.0
        if count < 1024.0:
            return f"{count:.1f}{unit}"

    return f"{count:.1f}PiB"
