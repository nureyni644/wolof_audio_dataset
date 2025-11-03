from airflow import DAG
from airflow.sdk import task
from airflow.operators.python import PythonOperator
# from airflow.operators import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import yt_dlp
import pandas as pd
from io import StringIO
S3_CONN_ID='aws_default'
BUCKET="osmanawsbucket"

name='workshop'



def upload_to_s3(file_name):


    s3_hook=S3Hook(aws_conn_id=S3_CONN_ID) 
    print(f"Uploading file {file_name} to S3 bucket {BUCKET}")
    # src = os.path.basename(file_name)

    dest = file_name
    dest = dest.replace(' ',"_")
        # supprimer les emojis du nom de fichier
    rts = dest.split('/')[0]
    dest = dest.replace(rts + '/',"")
    dest = ''.join(c for c in dest if c.isalnum() or c in (' ', '.', '_')).rstrip()
    dest = f"{name}/{rts}/{dest}"
    print(f"Downloaded file name: {file_name}")
    s3_hook.load_file(file_name, dest, bucket_name=BUCKET, replace=True)
    s3_hook.get_conn().close()
    print(f"File to remove: {file_name}")
    os.remove(file_name)

    


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
    




# video_url = 'https://youtu.be/r1AQF3YS1mw'
download_path = 'rts'

with DAG('s3_upload',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         schedule="*/45 * * * *",  
         default_args=default_args,
         catchup=False 
         ) as dag:

    @task
    def get_videos_list(channel_url,num_videos=2):
        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        filname = channel_url.split("@")[1].split("/")[0]
        content_to_download = s3.read_key(bucket_name=BUCKET, key=f"{filname}.csv")
        content_already_downloaded = s3.read_key(bucket_name=BUCKET, key=f"{filname}_downloaded.csv")
        urls_to_download = pd.read_csv(StringIO(content_to_download))
        urls_already_downloaded = pd.read_csv(StringIO(content_already_downloaded))
        urls_to_download = urls_to_download[~urls_to_download['video_url'].isin(urls_already_downloaded['video_url'])]
        # print(f"Videos to download: {len(urls_to_download)}")
        urls_to_download = urls_to_download[:num_videos]

        # urls_already_downloaded = pd.concat([urls_already_downloaded, urls_to_download], ignore_index=True)
        # print(f"Total downloaded videos updated to: {len(urls_already_downloaded)}")
        # print(urls_already_downloaded)
        # urls_already_downloaded.to_csv(filname + "_downloaded.csv", index=False)
        # s3.load_file(filname + "_downloaded.csv", key=f"{filname}_downloaded.csv", bucket_name=BUCKET, replace=True)
        s3.get_conn().close()
        # os.remove(filname + "_downloaded.csv")

        if len(urls_to_download) < num_videos:
            num_videos = len(urls_to_download)
        return urls_to_download['video_url'].tolist()[:num_videos]  
    @task
    def download_video(video_url, download_path="rts",channel='rts-radiotelevisionsenegalaise'):
        ydl_opts = {
            'format': 'bestvideo+bestaudio/best',
            'merge_output_format': 'mp4',  # Change to 'mkv' if preferred
            'outtmpl': f'{download_path}/%(title)s.%(ext)s',
            'noplaylist': True,  # Ensures only one video is downloaded
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:

            try:
                info = ydl.extract_info(video_url)

                file_name = ydl.prepare_filename(info)
                s3 = S3Hook(aws_conn_id=S3_CONN_ID)
                content = s3.read_key(key=f"{channel}_downloaded.csv", bucket_name=BUCKET)

                df = pd.read_csv(StringIO(content))
                new_row = pd.DataFrame({'video_url': [video_url]})
                df = pd.concat([df, new_row], ignore_index=True)
                df.to_csv(f"{channel}_downloaded.csv", index=False)
                s3.load_file(f"{channel}_downloaded.csv", key=f"{channel}_downloaded.csv", bucket_name=BUCKET, replace=True)
                os.remove(f"{channel}_downloaded.csv")
                s3.get_conn().close()
            except Exception as e:
                print(f"Error downloading {video_url}: {e}")
                return None

        upload_to_s3(file_name)

    
    # list_videos_task = PythonOperator(
    #     task_id='list_videos_task',
    #     python_callable=list_videosByChannel,
    #     op_kwargs={'channel_url': "https://www.youtube.com/@rts-radiotelevisionsenegalaise/videos"}
    # )
    # print(list_videos_task.output)
    # task_download = download_video.expand(video_url=list_videos_task.output)

    task_get_videos = get_videos_list(channel_url='https://www.youtube.com/@rts-radiotelevisionsenegalaise/videos')
    task_download = download_video.expand(video_url=task_get_videos)
