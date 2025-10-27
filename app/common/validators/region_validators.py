from app.common.exceptions.http_exception_wrapper import http_exception


def validate_region(region_id: int) -> int:
    """
    Function validates the region.
    Args:
        region_id (int): region id.
    Returns:
        int: region_id if everything is ok.
    Raises:
        400 HTTP if region id is unacceptable
    """

    if region_id == 143111:
        raise http_exception(
            400,
            msg="Region is unavailable",
            _input={"region_id": region_id},
            _detail={"Unacceptable regions": [143111]},
        )
    return region_id
