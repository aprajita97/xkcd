from typing import Optional
import datetime
import requests

URL = 'https://xkcd.com/info.0.json'

def _is_latest_comic(comic_date_str: str) -> bool:
    today = datetime.date.today()
    comic_date = datetime.datetime.strptime(comic_date_str, "%Y-%m-%d").date()
    if comic_date == today:
        return True
    return False

def _fetch_comic_of_the_day(url=URL, timeout=10) -> Optional[dict]:
    """
    Description:
    Makes an API call with error handling and returns the response content 
    if the comic is of current date.

    Args:
    url (str): The URL of the API endpoint.
    timeout (int, optional): The timeout in seconds for the request. Defaults to 10.

    Returns:
    dict or None: The response content as a dictionary (JSON) or None if an error occurs.

    Raises:
        requests.exceptions.RequestException: If any error occurs during the request.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        # return response.json()
        comic_date_str = response.json()['year'] + '-' + response.json()['month'] + '-' + response.json()['day']
        if not _is_latest_comic(comic_date_str):
            raise Exception(f"Comic is from date {comic_date_str}")
        return response.json()
    except requests.exceptions.RequestException as e:
        raise e(f"Error making API call: {e}")
