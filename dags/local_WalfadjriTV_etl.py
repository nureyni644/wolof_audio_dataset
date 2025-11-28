from airflow import DAG
from airflow.sdk import task
# from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import yt_dlp
import pandas as pd
import torchaudio
import hashlib
from dotenv import load_dotenv
from huggingface_hub import HfApi, login
load_dotenv()
DATA_DIR = '/usr/local/airflow/'

# @task
def splitAndSaveToHF(file_name:str, segment_duration_sec=25,overlap_sec=2):
    """
    Split audio file into segments of 25 seconds each and save them as separate files.
    Arguments:
        file_name (str): Path to the audio file.
    Returns:
        None
    """
    # print(f"Splitting file: {file_name.split('.mp3')}")
    assert os.path.exists(file_name), f"File {file_name} does not exist."
    waveform, sample_rate = torchaudio.load(file_name)
    os.remove(file_name)
    total_duration_sec = waveform.shape[1] / sample_rate
    # segments = []
    start_sec = 0
    unique_id = hashlib.sha1(file_name.encode()).hexdigest()[:8]
    while start_sec < total_duration_sec:
        end_sec = min(start_sec + segment_duration_sec, total_duration_sec)
        start_sample = int(start_sec * sample_rate)
        end_sample = int(end_sec * sample_rate)
        segment_waveform = waveform[:, start_sample:end_sample]
        # segments.append((segment_waveform, sample_rate))
        
        os.mkdir(unique_id) if not os.path.exists(unique_id) else None
        file_name_segment = f"{unique_id}/{unique_id}_segment_{int(start_sec)}s_to_{int(end_sec)}s.mp3"
        torchaudio.save(f"{file_name_segment}", segment_waveform, sample_rate)
        
        if end_sec == total_duration_sec:
            break
        start_sec += segment_duration_sec - overlap_sec
    
    # base_name = os.path.splitext(os.path.basename(file_name))[0]
    return unique_id

    # upload_to_hf(unique_id, commit_message=f"Add the segments of {base_name}")
    
    

@task
def upload_to_hf(directory_path,commit_message="Add audio segments"):
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    api = HfApi()
    hf_token = os.getenv("HF_TOKEN")
    login(hf_token)
    repo_id = "osma77/wolof-yt-audio"
    api.create_repo(
        repo_id=repo_id, exist_ok=True,
        repo_type="dataset",private=True
    )
    api.upload_folder(
        folder_path=directory_path,
        repo_id=repo_id,
        repo_type="dataset",
        path_in_repo=f'WalfadjriTV/{directory_path}',
        commit_message=f"Add audio {directory_path} segments",
    )
    for file in os.listdir(directory_path):
        os.remove(os.path.join(directory_path, file))
    os.rmdir(directory_path)

    print(f"Uploaded {directory_path} to Hugging Face dataset repo {repo_id}.")


    


default_args = {
    'owner': 'nureyni',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('download_WalfadjriTV',
         start_date=datetime(2025, 11, 13),
         max_active_runs=1,
         schedule="*/20 * * * *",  
         default_args=default_args,
         catchup=False 
         ) as dag:

    @task
    def get_videos_list(channel_url,num_videos=3):
      
        filname = channel_url.split("@")[1].split("/")[0]
        urls_to_download = pd.read_csv(F"{DATA_DIR}{filname}.csv")
        urls_already_downloaded = pd.read_csv(F"{DATA_DIR}{filname}_downloaded.csv")
        urls_to_download = urls_to_download[~urls_to_download['video_url'].isin(urls_already_downloaded['video_url'])]


        if len(urls_to_download) < num_videos:
            num_videos = len(urls_to_download)
        return urls_to_download['video_url'].tolist()[:num_videos]  
    @task
    def download_video(video_url, download_path="walf",channel='WalfadjriTV'):
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

                file_name = info["requested_downloads"][0]["filepath"]
            
                df = pd.read_csv(f"{DATA_DIR}{channel}_downloaded.csv")
                new_row = pd.DataFrame({'video_url': [video_url]})
                df = pd.concat([df, new_row], ignore_index=True)
                df.to_csv(f"{DATA_DIR}{channel}_downloaded.csv", index=False)

            except Exception as e:
                print(f"^^^^Error downloading {video_url}: {e}")
                df = pd.read_csv(f"{DATA_DIR}{channel}_downloaded.csv")
                df = df[df['video_url'!=video_url]]
                df.to_csv(f"{DATA_DIR}{channel}_downloaded.csv", index=False)
                return None
            unique_uui =splitAndSaveToHF(file_name, segment_duration_sec=25, overlap_sec=2)
            return unique_uui
            
            

    
   

    task_get_videos = get_videos_list(channel_url='https://www.youtube.com/@WalfadjriTV/videos', num_videos=10)
    task_download = download_video.expand(video_url=task_get_videos)
    task_upload = upload_to_hf.expand(directory_path=task_download)
