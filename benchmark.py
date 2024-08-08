import subprocess
import time

# Define the cURL command
curl_command = (
    'curl -s -o /dev/null -X GET "http://localhost:8000/metadata/details?symbol=AAPL&start_date=2023-01-01&end_date=2023-12-31" '
    '-H "X-API-KEY: 929ad1fa-57de-46f8-a3c7-7b45c3899605"'
)

# Number of requests to make
num_requests = 1000

# Track the start time
start_time = time.time()

# Loop to execute the cURL command multiple times
for i in range(num_requests):
    # Execute the cURL command
    subprocess.run(curl_command, shell=True)

    # Print progress every 1000 requests
    if (i + 1) % 100 == 0:
        print(f"{i + 1} requests completed")

# Calculate and print the total time taken
end_time = time.time()
total_time = end_time - start_time
print(f"Completed {num_requests} requests in {total_time:.2f} seconds")
