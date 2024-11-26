import os
import subprocess
import time

# Path to the script you want to run
script_path = r"Q:\Europe-LAND\scripts\a_ALL_download_files.py"

# Retry interval in seconds
retry_interval = 60

x = True
while x == True:
    try:
        # Attempt to run the script
        print(f"Attempting to run {script_path}...")
        subprocess.run(["python", script_path], check=True)
        print(f"{script_path} ran successfully.")
        # if os.path.exists(r"Q:\Europe-LAND\data\vector\IACS\PT\download\parcelas_done.txt"):
        # if os.path.exists(r"Q:\Europe-LAND\data\vector\IACS\PT\download\ocupacoes_solo_done.txt"):
        if os.path.exists(r"Q:\Europe-LAND\data\vector\IACS\PT\download\culturas_done.txt"):
            x = False
            break  # Exit loop if the script runs successfully
    except subprocess.CalledProcessError as e:
        # Handle script failure
        print(f"Script failed with error: {e}. Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)