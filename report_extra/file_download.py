import os
import re
import subprocess
from typing import List
from googleapiclient.discovery import build

# Constants
API_KEY = os.getenv("YOUTUBE_API_KEY")
MAX_DURATION_SECONDS = 3 * 60 * 60  # 3 hours
PROXY_URL = os.getenv("PROXY_URL")  # Example: http://user:pass@host:port

def parse_iso_duration(iso_duration: str) -> int:
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def get_video_durations(video_ids: List[str]) -> dict:
    youtube = build("youtube", "v3", developerKey=API_KEY)
    response = youtube.videos().list(part="contentDetails", id=",".join(video_ids)).execute()

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

    print("Eligible:", eligible)
    print("Ineligible:", ineligible)
    return eligible


def download_video(video_id: str, download_audio_only: bool = False):
    url = f"https://www.youtube.com/watch?v={video_id}"

    output_template = "%(title)s.%(ext)s"
    format_code = "bestaudio" if download_audio_only else "best"

    command = [
        "yt-dlp",
        "-f", format_code,
        "--output", output_template,
        url
    ]

    if PROXY_URL:
        command.extend(["--proxy", PROXY_URL])

    print(f"Downloading {url}...")
    subprocess.run(command, check=True)


# 🎬 Example Usage
if __name__ == "__main__":
    video_ids = [
        "dQw4w9WgXcQ",  # < 3 hours
        "5qap5aO4i9A",  # > 3 hours
    ]

    eligible = filter_eligible_videos(video_ids)
    for vid in eligible:
        download_video(vid, download_audio_only=False)  # change to True for audio only
