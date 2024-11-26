import os
import urllib.request
import zipfile
import glob
import ssl

######### SPAIN ##########
download_lst = [
    f"https://www.fega.gob.es/atom/2024/ld_2024/13%20-%20CIUDAD%20REAL/13{i:03d}_ld_2023_20230116_gpkg.zip" for
    i in range(35, 103)]
download_lst.append("https://www.fega.gob.es/atom/2023/ld_2023/13%20-%20CIUDAD%20REAL/13900_ld_2023_20230116_gpkg.zip")

ssl._create_default_https_context = ssl._create_unverified_context
output_lst = [fr"Q:\Europe-LAND\data\vector\IACS\ES_temp\downloads\{os.path.basename(url)}" for url in download_lst]
for i, download in enumerate(download_lst):
    print(f"DL {i+1}/{len(download_lst)}, {download}")
    urllib.request.urlretrieve(download, output_lst[i])

districts = ["CIUDAD REAL"]
#
for district in districts:
    print(district)
    unzip_list = glob.glob(fr"Q:\Europe-LAND\data\vector\IACS\ES_temp\{district}\*.zip")

    for i, path in enumerate(unzip_list):
        print(f"{i}/{len(unzip_list)} - UZ {path}")
        ## Get folder
        folder = rf"Q:\Europe-LAND\data\vector\IACS\ES_temp\{district}"

        ## Unzip
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(folder)