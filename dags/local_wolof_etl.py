from airflow import DAG
from airflow.sdk import task
from airflow.operators.python import PythonOperator
# from airflow.operators import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import yt_dlp
import pandas as pd


DATA_DIR = '/usr/local/airflow/data/'




def upload_to_s3(file_name):
    print(f"Uploading file {file_name} ")

    


default_args = {
    'owner': 'nureyni',
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
    



download_path = 'rts'

with DAG('local_upload',
         start_date=datetime(2025, 11, 13),
         max_active_runs=1,
         schedule="*/15 * * * *",  
         default_args=default_args,
         catchup=False 
         ) as dag:

    @task
    def get_videos_list(channel_url,num_videos=7):
      
        filname = channel_url.split("@")[1].split("/")[0]
        urls_to_download = pd.read_csv(F"{DATA_DIR}{filname}.csv")
        urls_already_downloaded = pd.read_csv(F"{DATA_DIR}{filname}_downloaded.csv")
        urls_to_download = urls_to_download[~urls_to_download['video_url'].isin(urls_already_downloaded['video_url'])]


        if len(urls_to_download) < num_videos:
            num_videos = len(urls_to_download)
        return urls_to_download['video_url'].tolist()[:num_videos]  
    @task
    def download_video(video_url, download_path="rts",channel='rts-radiotelevisionsenegalaise'):
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',   
                'preferredquality': '192',
            }],
            # 'format': 'bestvideo+bestaudio/best',
            # 'merge_output_format': 'mp4',  # Change to 'mkv' if preferred
            'outtmpl': f'{download_path}/%(title)s.%(ext)s',
            'noplaylist': True,  
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:

            try:
                info = ydl.extract_info(video_url)

                file_name = ydl.prepare_filename(info)
            
                df = pd.read_csv(f"{DATA_DIR}{channel}_downloaded.csv")
                new_row = pd.DataFrame({'video_url': [video_url]})
                df = pd.concat([df, new_row], ignore_index=True)
                df.to_csv(f"{DATA_DIR}{channel}_downloaded.csv", index=False)

            except Exception as e:
                print(f"Error downloading {video_url}: {e}")
                return None

        upload_to_s3(file_name)

    
   

    task_get_videos = get_videos_list(channel_url='https://www.youtube.com/@rts-radiotelevisionsenegalaise/videos')
    task_download = download_video.expand(video_url=task_get_videos)
