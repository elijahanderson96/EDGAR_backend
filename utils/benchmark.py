import asyncio
import aiohttp
import time

async def fetch(session, endpoint_url, api_key, symbol):
    """
    Sends a single GET request asynchronously.
    """
    headers = {'X-API-KEY': api_key}
    params = {'symbol': symbol}

    try:
        async with session.get(endpoint_url, headers=headers, params=params) as response:
            return await response.json()
    except Exception as e:
        return {'error': str(e)}

async def make_requests_concurrently(endpoint_url: str, api_key: str, symbols: list, num_requests: int = 1000):
    """
    Makes `num_requests` to the specified endpoint concurrently and times the total duration.

    Parameters:
        endpoint_url (str): The URL of the endpoint.
        api_key (str): The API key for authentication.
        symbols (list): A list of symbols to use as query parameters.
        num_requests (int): The number of requests to make (default: 1000).

    Returns:
        dict: A dictionary containing the total elapsed time and a list of responses.
    """
    start_time = time.time()
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in range(num_requests):
            symbol = symbols[i % len(symbols)]  # Cycle through symbols
            tasks.append(fetch(session, endpoint_url, api_key, symbol))

        # Run tasks concurrently
        responses = await asyncio.gather(*tasks)

    end_time = time.time()
    elapsed_time = end_time - start_time

    return {
        "total_time": elapsed_time,
        "average_time_per_request": elapsed_time / num_requests,
        "responses": responses
    }

# Example usage
if __name__ == "__main__":
    endpoint = 'http://localhost:8000/metadata'
    api_key = '52b205d5-93e0-4d68-a202-8b081638de4e'
    symbols_list = ['BAC', 'BA', 'AMZN', 'MSFT', 'CVX', 'PFE', 'JNJ', 'HD', 'LMT', 'ABBV', 'NFLX', 'CRM', 'KO', 'UNH', 'XOM', 'V']

    result = asyncio.run(make_requests_concurrently(endpoint, api_key, symbols_list, num_requests=1000))

    print(f"Total Time: {result['total_time']} seconds")
    print(f"Average Time per Request: {result['average_time_per_request']} seconds")
