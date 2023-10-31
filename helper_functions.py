def list_geospatial_data_in_dir(dir):

    import glob

    types = ('**/*.gpkg', '**/*.gdb', '**/*.shp', '**/*.geojson', '*.gpkg', '*.gdb', '*.shp', '*.geojson')  # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(rf"{dir}\{files}"))


    return files_grabbed

def list_csv_files_in_dir(dir):

    import glob

    types = ('**/*.csv', '*.csv')  # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(rf"{dir}\{files}"))

    return files_grabbed



def create_folder(directory):
    """
    Tries to create a folder at the specified location. Path should already exist (excluding the new folder).
    If folder already exists, nothing will happen.
    :param directory: Path including new folder.
    :return: Creates a new folder at the specified location.
    """

    import os
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)


def most_common(lst):
    return max(set(lst), key=lst.count)


def get_year_from_path(path):
    import re
    group = re.search(r"(\d{4})", path)
    if not group:
        group = re.search(r"(\d{2})", path)

    if group:
        year = group.group(1)
    else:
        year = None

    return year


