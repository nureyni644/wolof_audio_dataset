# from airflow import DAG
from airflow.sdk import task, dag
from airflow.operators.python import PythonOperator
# from airflow.operators import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import yt_dlp
import pandas as pd

S3_CONN_ID='aws_default'
BUCKET="osmanawsbucket"

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def list_videosByChannel(channel_url):
    ydl_opts = {
        'ignoreerrors': True,
        'quiet': True,
        'skip_download': True,
        'extract_flat': True
    }

    video_urls = []
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        result = ydl.extract_info(channel_url, download=False)
    if result is not None:
        for entry in result.get('entries', []):
            if "xibaar" in entry.get('title','').lower():
                video_urls.append(entry.get('url')
                    # 'title': entry.get('title'),
                
                )
    print(f"Found {len(video_urls)} videos in channel.")
    return video_urls


@dag(
    dag_id='yt_videos',
    default_args=default_args,
    description='List YouTube videos from a channel and upload their URLs to S3',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
)
def yt_videos_dag():

    @task
    def upload_to_s3(file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket"""
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        if object_name is None:
            object_name = os.path.basename(file_name)
        if (("downloaded" in file_name) and  not s3.check_for_key(key=object_name, bucket_name=bucket)):
            s3.load_file(
                filename=file_name,
                key=object_name,
                bucket_name=bucket,
                replace=True
            )
        if (("downloaded" not in file_name)):
            s3.load_file(
                filename=file_name,
                key=object_name,
                bucket_name=bucket,
                replace=True
            )
        # close
        s3.get_conn().close()
        print(f"Uploaded {file_name} to s3://{bucket}/{object_name}")
        # os.remove(file_name)

    @task
    def process_channel(channel_url):
        video_urls = list_videosByChannel(channel_url)
        df = pd.DataFrame(video_urls, columns=['video_url'])
        df_downloaded = pd.DataFrame([], columns=['video_url'])
        file_name = f"{channel_url.split('@')[1].split('/')[0]}.csv"
        file_name_downloaded = f"{channel_url.split('@')[1].split('/')[0]}_downloaded.csv"
        
        df.to_csv(file_name, index=False)
        df_downloaded.to_csv(file_name_downloaded, index=False)
        return [file_name, file_name_downloaded]
    @task
    def delete_local_file(file_name):
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Deleted local file: {file_name}")
        else:
            print(f"File not found, could not delete: {file_name}")
        
    task_list_videos = process_channel(channel_url='https://www.youtube.com/@rts-radiotelevisionsenegalaise/videos')
    
    task_upload_s3 = upload_to_s3.expand(file_name=task_list_videos, bucket=[BUCKET]*2)
    task_delete_local = delete_local_file.expand(file_name=task_list_videos)
    # task_list_videos >> task_upload_s3
    task_upload_s3 >> task_delete_local

yt_videos_dag()