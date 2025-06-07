import os
import re
from googleapiclient.discovery import build
from datetime import timedelta
from typing import List

# Constants
API_KEY = os.getenv("YOUTUBE_API_KEY")  # Set this in your environment
MAX_DURATION_SECONDS = 3 * 60 * 60  # 3 hours

def parse_iso_duration(iso_duration: str) -> int:
    """
    Convert ISO 8601 duration to seconds.
    Example: 'PT2H30M15S' → 9015 seconds
    """
    match = re.match(
        r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?',
        iso_duration
    )
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def get_video_durations(video_ids: List[str]) -> dict:
    """
    Get durations for multiple YouTube video IDs.
    """
    youtube = build("youtube", "v3", developerKey=API_KEY)
    response = youtube.videos().list(
        part="contentDetails",
        id=",".join(video_ids)
    ).execute()

    durations = {}
    for item in response.get("items", []):
        video_id = item["id"]
        duration_iso = item["contentDetails"]["duration"]
        duration_seconds = parse_iso_duration(duration_iso)
        durations[video_id] = duration_seconds

    return durations


def filter_eligible_videos(video_ids: List[str]) -> List[str]:
    durations = get_video_durations(video_ids)
    eligible = [vid for vid, dur in durations.items() if dur <= MAX_DURATION_SECONDS]
    ineligible = [vid for vid, dur in durations.items() if dur > MAX_DURATION_SECONDS]

    print("Eligible for download:", eligible)
    print("Excluded (too long):", ineligible)
    return eligible


# Example usage
if __name__ == "__main__":
    # Example list of video IDs
    video_ids = [
        "dQw4w9WgXcQ",  # ~3min
        "5qap5aO4i9A",  # ~24hr lo-fi stream (example of long video)
    ]

    eligible_videos = filter_eligible_videos(video_ids)
