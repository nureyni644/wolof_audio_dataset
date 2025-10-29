from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import yt_dlp
import subprocess
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

BUCKET_NAME = "osmanawsbucket"
FILE_NAME = "wolof.txt"
def read_from_s3(**context):
    ti = context['ti']
    output_files = ti.xcom_pull(task_ids='download_all_wolof_audio')
    print("📥 Lecture du fichier depuis S3...")
    hook = S3Hook(aws_conn_id="aws_default")

    try:
        keys = hook.list_keys(bucket_name=BUCKET_NAME)
        if keys:
            print("✅ Fichiers trouvés dans le bucket :", keys)
        else:
            print("🟡 Aucun fichier trouvé dans le bucket (mais accès OK).")
    except Exception as e:
        print("❌ Erreur lors du listage :", e)

OUTPUT_DIR = "./data/wavs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Liste de vidéos YouTube wolof à récupérer
YOUTUBE_URLS = [
    # "https://youtu.be/QIiwPZP1C7I",
    "https://www.youtube.com/shorts/k_3q870N8T8?feature=share"
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
    
        subprocess.run([
            "ffmpeg", "-y", "-i", input_file, "-ar", "16000", "-ac", "1", output_file
        ])
        os.remove(input_file)
        
        return output_file

def save_to_s3(**context):
    ti = context['ti']
    files = ti.xcom_pull(task_ids='download_all_wolof_audio')

    if not files:
        print("Aucun fichier à uploader")
        return

    hook = S3Hook(aws_conn_id="aws_default")

    for local_file in files:
        filename = os.path.basename(local_file)
        print(f"Upload {local_file} vers S3 bucket {BUCKET_NAME} sous le nom {filename}")
        try:
            hook.load_file(
                filename=local_file,        
                key=filename,               
                bucket_name=BUCKET_NAME,
                replace=True          
            )
            print(f"{filename} uploadé avec succès")
        except Exception as e:
            print(f"Erreur lors de l'upload de {filename} :", e)

def download_all_videos():
    output_files = []
    for url in YOUTUBE_URLS:
        output_file = download_audio(url)
        if output_file:
            output_files.append(output_file)
    return output_files

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
        python_callable=download_all_videos,
    )

    task_save_to_s3 = PythonOperator(
        task_id = "save_to_s3",
        python_callable = save_to_s3
    )

    read_task = PythonOperator(
        task_id="read_from_s3",
        python_callable=read_from_s3
    )
    task_download_audio >> task_save_to_s3>>read_task

    