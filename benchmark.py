import aiohttp
import asyncio
import time

async def make_request(session, url, headers):
    async with session.get(url, headers=headers) as response:
        await response.read()  # ensure the response is fully consumed

async def run_load_test(num_requests):
    url = "http://localhost:8000/metadata/balance_sheet?symbol=AAPL"
    headers = {'X-API-KEY': '929ad1fa-57de-46f8-a3c7-7b45c3899605'}

    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, url, headers) for _ in range(num_requests)]
        await asyncio.gather(*tasks)

# Number of requests to make
num_requests = 1000

# Track the start time
start_time = time.time()

# Run the load test
asyncio.run(run_load_test(num_requests))

# Calculate and print the total time taken
end_time = time.time()
total_time = end_time - start_time
print(f"Completed {num_requests} requests in {total_time:.2f} seconds")
