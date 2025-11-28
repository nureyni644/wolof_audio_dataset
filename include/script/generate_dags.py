import shutil
import json
import os
import fileinput

TEMPLATE_PATH_2 = 'include/template/local_wolof_etl.py'
TEMPLATE_PATH_1 = 'include/template/local_yt_videos.py'
DATA_PATH = 'include/data/'
DAGS_PATH = 'dags/'

for file_name in os.listdir(DATA_PATH):
    assert file_name.endswith('.json'), f"Only JSON config files are allowed in the data directory: {DATA_PATH}"
    
    config = json.load(open(os.path.join(DATA_PATH, file_name)))
    new_dag_file2 = os.path.join(DAGS_PATH, f"local_{config['channel_name']}_etl.py")
    new_dag_file1 = os.path.join(DAGS_PATH, f"local_yt_{config['channel_name']}.py")
    shutil.copyfile(TEMPLATE_PATH_2, new_dag_file2)
    shutil.copyfile(TEMPLATE_PATH_1, new_dag_file1)
    replacements = {
        'CHANNEL_NAME': config['channel_name'],
        'YOUTUBE_CHANNEL_URL': config['youtube_channel_url'],
        'FILTER_KEYWORDS': config['filter_keyword'],
        'DOWNLOAD_PATH': config['download_path'],
        'SCHEDULE_CRON_EXPRESSION': config['schedule_cron_expression'],
        'PATH_IN_REPO':config['channel_name']
    }
    for new_dag_file in [new_dag_file1, new_dag_file2]:
        with fileinput.FileInput(new_dag_file, inplace=True) as file:
            for line in file:
                for key, value in replacements.items():
                    line = line.replace(key, value)
                print(line, end='')
