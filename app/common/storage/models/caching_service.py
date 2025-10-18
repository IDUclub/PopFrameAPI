from pathlib import Path


class CachingService:
    """Class for caching interface"""

    def __init__(self, cache_path: Path) -> None:
        """
        Function initialize caching service class
        Args:
            cache_path: path to cache file
        Returns:
            None
        """

        self.caching_path = cache_path
        self.caching_path.mkdir(parents=True, exist_ok=True)
