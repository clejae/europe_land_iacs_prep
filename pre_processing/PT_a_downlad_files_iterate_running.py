import subprocess
import time
import os
from os.path import dirname, abspath

WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

# Path to the script you want to run
script_path = os.path.join("scripts", "pre_processing", "PT_a_download_files.py") #r"Q:\Europe-LAND\scripts\a_ALL_download_files.py"

# Retry interval in seconds
retry_interval = 20

x = True
while x == True:
    try:
        # Attempt to run the script
        print(f"Attempting to run {script_path}...")
        subprocess.run(["python", script_path], check=True)
        print(f"{script_path} ran successfully.")
        # if os.path.exists(os.path.join("data", "vector", "IACS", "PT", "download", "parcelas_done.txt")):
        # if os.path.exists(os.path.join("data", "vector", "IACS", "PT", "download", "ocupacoes_solo_done.txt")):
        if os.path.exists(os.path.join("data", "vector", "IACS", "PT", "download", "culturas_done.txt")):
            x = False
            break  # Exit loop if the script runs successfully
    except subprocess.CalledProcessError as e:
        # Handle script failure
        print(f"Script failed with error: {e}. Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)