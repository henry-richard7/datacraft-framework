from datetime import datetime


def file_name_generator(save_file_name: str) -> str:
    """
    Generate a dynamic file name by replacing date placeholders with actual formatted dates.

    This function replaces specific tokens in the input string with the current date in the corresponding format:
    - Replaces `"YYYYMMDD"` with today's date in `YYYYMMDD` format.
    - Replaces `"YYYYMM"` with today's date in `YYYYMM` format.
    - Replaces `"YYYY"` with the current year.

    The replacement is done in priority order: longest pattern first to avoid partial overlaps.

    Args:
        save_file_name (str): Original file name that may contain date placeholder tokens.

    Returns:
        str: A new file name with all applicable date tokens replaced by current values.

    Examples:
        >>> file_name_generator("data_YYYYMMDD.csv")
        'data_20250405.csv'

        >>> file_name_generator("report_YYYYMM.xlsx")
        'report_202504.xlsx'
    """
    today_date = datetime.now()

    if "YYYYMMDD" in save_file_name:
        save_file_name = save_file_name.replace(
            "YYYYMMDD", today_date.strftime("%Y%m%d")
        )
    elif "YYYYMM" in save_file_name:
        save_file_name = save_file_name.replace(
            "YYYYMM",
            today_date.strftime("%Y%m"),
        )
    elif "YYYY" in save_file_name:
        save_file_name = save_file_name.replace("YYYY", today_date.strftime("%Y"))
    else:
        save_file_name = save_file_name

    return save_file_name
