# from airflow import DAG
from airflow.sdk import task, dag
from airflow.operators.python import PythonOperator
# from airflow.operators import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import yt_dlp
import pandas as pd

DATA_DIR = '/usr/local/airflow/data/'
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
    dag_id='local_yt_videos',
    default_args=default_args,
    description='List YouTube videos from a channel and upload their URLs to S3',
    schedule='@daily',
    start_date=datetime(2025, 11, 13),
    catchup=False,
    max_active_runs=1
)
def yt_videos_dag():
    
    @task
    def process_channel(channel_url):
        video_urls = list_videosByChannel(channel_url)
        df = pd.DataFrame(video_urls, columns=['video_url'])
        df_downloaded = pd.DataFrame([], columns=['video_url'])
        file_name = f"{channel_url.split('@')[1].split('/')[0]}.csv"
        file_name_downloaded = f"{channel_url.split('@')[1].split('/')[0]}_downloaded.csv"
        
        df.to_csv(f"{DATA_DIR}{file_name}", index=False)
        print(f"==========Created local file========:{os.path.exists(f"{DATA_DIR}{file_name_downloaded}")}")
        if not os.path.exists(f"{DATA_DIR}{file_name_downloaded}"):
            print("File exists, reading existing URLs.")
            df_downloaded.to_csv(f"{DATA_DIR}{file_name_downloaded}", index=False)
        return [file_name, file_name_downloaded]
    
        
    task_list_videos = process_channel(channel_url='https://www.youtube.com/@rts-radiotelevisionsenegalaise/videos')

yt_videos_dag()