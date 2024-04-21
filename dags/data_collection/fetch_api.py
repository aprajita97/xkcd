from typing import Optional
import requests

URL = 'https://xkcd.com/info.0.json'

def _fetch_comic_of_the_day(url=URL, timeout=10) -> Optional[dict]:
    """
    Description:
    Makes an API call with error handling and returns the response content.

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
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making API call: {e}")
        return None
