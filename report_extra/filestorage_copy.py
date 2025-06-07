import boto3
from pathlib import Path

# S3 session setup
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
s3 = session.resource("s3")
SERVICE_BUCKET = os.getenv("SERVICE_BUCKET")

# Upload to Service Bucket
def upload_to_service_bucket(local_path: str, s3_key: str):
    """Upload a file from local storage to the service S3 bucket."""
    s3.meta.client.upload_file(local_path, SERVICE_BUCKET, s3_key)

# Copy to Consumer Bucket
def copy_to_consumer_bucket(source_key: str, consumer_bucket: str, destination_key: str):
    """Copy a file from the service bucket to a consumer's bucket."""
    copy_source = {
        'Bucket': SERVICE_BUCKET,
        'Key': source_key
    }
    s3.meta.client.copy(copy_source, consumer_bucket, destination_key)

# Full Updated Endpoint (Simplified)
@app.post("/download", response_model=VideoResponse)
def download_videos(request: VideoRequest):
    if not YOUTUBE_API_KEY:
        raise HTTPException(status_code=500, detail="YouTube API key is not set")
    
    durations = get_video_durations(request.video_ids)
    eligible = [vid for vid, dur in durations.items() if dur <= MAX_DURATION_SECONDS]
    excluded = [vid for vid in request.video_ids if vid not in eligible]

    download_status = {}
    for vid in eligible:
        success = download_video(vid, audio_only=request.audio_only)
        if success:
            # Assume yt-dlp saves as: downloads/<title>.<ext>
            file_path = get_latest_download_file("downloads")  # Helper to find the latest file
            filename = os.path.basename(file_path)
            s3_key = f"downloads/{filename}"

            try:
                upload_to_service_bucket(file_path, s3_key)
                # Replace 'consumer-bucket-name' with actual one from user config
                copy_to_consumer_bucket(s3_key, consumer_bucket="consumer-bucket-name", destination_key=s3_key)
                download_status[vid] = "uploaded & copied"
            except Exception as e:
                download_status[vid] = f"upload failed: {str(e)}"
        else:
            download_status[vid] = "download failed"

    return VideoResponse(
        eligible=eligible,
        excluded=excluded,
        download_status=download_status
    )

#  Helper to Get Latest File in Downloads
def get_latest_download_file(directory: str) -> str:
    files = sorted(Path(directory).glob("*"), key=os.path.getmtime, reverse=True)
    return str(files[0]) if files else ""
  
