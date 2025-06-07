import time
import uuid
from collections import deque
from typing import Dict

# In-memory storage
request_log: Dict[str, dict] = {}

# Throttling configuration
MAX_REQUESTS_PER_MINUTE = 5
THROTTLE_WINDOW_SECONDS = 60

# Request timestamps queue for throttling
request_timestamps = deque()


def throttle_check() -> bool:
    """Check if the request exceeds rate limit."""
    current_time = time.time()

    # Remove old timestamps outside the window
    while request_timestamps and current_time - request_timestamps[0] > THROTTLE_WINDOW_SECONDS:
        request_timestamps.popleft()

    if len(request_timestamps) >= MAX_REQUESTS_PER_MINUTE:
        return False  # Throttle limit exceeded

    request_timestamps.append(current_time)
    return True


def save_request(request_data: dict) -> str:
    """Save request and generate a job ID."""
    job_id = str(uuid.uuid4())
    request_log[job_id] = {
        "timestamp": time.time(),
        "data": request_data,
        "status": "queued"
    }
    return job_id


def handle_request(request_data: dict):
    if not throttle_check():
        return {"status": "error", "message": "Too many requests, throttled."}

    job_id = save_request(request_data)
    return {"status": "accepted", "job_id": job_id}


# Example Usage
if __name__ == "__main__":
    for i in range(7):
        response = handle_request({"user": f"user_{i}", "action": "process"})
        print(response)
        time.sleep(5)  # simulate some delay
