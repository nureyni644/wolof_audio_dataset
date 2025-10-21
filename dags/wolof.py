from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import yt_dlp
import subprocess
import os

# Dossier où seront stockés les WAV
OUTPUT_DIR = "./data/wavs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Liste de vidéos YouTube wolof à récupérer
YOUTUBE_URLS = [
    "https://youtu.be/QIiwPZP1C7I",
    "https://youtu.be/sJvYJkR4TBo",
    # Ajouter d'autres URLs ici
]

def download_audio(video_url, output_dir=OUTPUT_DIR):
    """
    Télécharge l'audio depuis YouTube et convertit en WAV 16kHz mono
    """
    # Télécharger audio mp3/m4a
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": f"{output_dir}/%(id)s.%(ext)s",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(video_url, download=True)
        video_id = info_dict['id']
        ext = info_dict['ext']
        input_file = os.path.join(output_dir, f"{video_id}.{ext}")
        output_file = os.path.join(output_dir, f"{video_id}.wav")
        # Conversion en WAV 16kHz mono
        subprocess.run([
            "ffmpeg", "-y", "-i", input_file, "-ar", "16000", "-ac", "1", output_file
        ])
        # Optionnel : supprimer le fichier original mp3/m4a
        os.remove(input_file)

def download_all_videos():
    for url in YOUTUBE_URLS:
        download_audio(url)


# Définition du DAG
with DAG(
    dag_id="download_wolof_youtube_audio",
    start_date=datetime(2025, 10, 20),
    schedule="@daily",
    catchup=False,
    tags=["wolof", "audio", "youtube"]
) as dag:

    task_download_audio = PythonOperator(
        task_id="download_all_wolof_audio",
        python_callable=download_all_videos
    )

    @task
    def play_audio_files(file_names):
        from IPython.display import Audio, display
        for file_name in file_names:
            display(Audio(filename=file_name))
    @task
    def split_audio(file_name:str):
        from IPython.display import Audio
        # read a file wav extension
        import pandas as pd
        pd
        pass
    task_play_audio = play_audio_files([os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith('.wav')])
    task_download_audio >> task_play_audio
        
