import requests
import time
import concurrent.futures


def fetch_old_db_benchmark(url):
    start_time = time.time()
    response = requests.get(url)
    duration = time.time() - start_time
    return response.json(), duration


def fetch_new_db_benchmark(url):
    start_time = time.time()
    response = requests.get(url)
    duration = time.time() - start_time
    return response.json(), duration


def main():
    old_db_url = "http://127.0.0.1:8000/old_db/benchmark"
    new_db_url = "http://127.0.0.1:8000/new_db/benchmark"
    num_requests = 100  # Adjust the number of requests as needed

    old_db_durations = []
    new_db_durations = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Old DB Benchmark
        old_db_futures = [executor.submit(fetch_old_db_benchmark, old_db_url) for _ in range(num_requests)]
        for future in concurrent.futures.as_completed(old_db_futures):
            response, duration = future.result()
            old_db_durations.append(duration)

        # New DB Benchmark
        new_db_futures = [executor.submit(fetch_new_db_benchmark, new_db_url) for _ in range(num_requests)]
        for future in concurrent.futures.as_completed(new_db_futures):
            response, duration = future.result()
            new_db_durations.append(duration)

    avg_old_db_duration = sum(old_db_durations) / len(old_db_durations)
    avg_new_db_duration = sum(new_db_durations) / len(new_db_durations)

    print(f"Average duration for old DB benchmark: {avg_old_db_duration} seconds")
    print(f"Average duration for new DB benchmark: {avg_new_db_duration} seconds")


if __name__ == "__main__":
    main()
