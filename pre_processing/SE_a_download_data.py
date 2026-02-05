import requests
import os
from os.path import dirname, abspath

WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)


## Sweden
url = "http://epub.sjv.se/inspire/inspire/wfs"

for year in range(2024, 2025):
    # Define parameters
    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "1.0.0",
        "TYPENAMES": "inspire:arslager_skifte",
        "outputFormat": "shape-zip",
        "CQL_FILTER": f"arslager='{year}' and geom is not null",
        "format_options": "CHARSET:UTF-8",
    }

    # Make the request
    response = requests.get(url, params=params)

    # Save response content if the request was successful
    if response.status_code == 200:
        with open(os.path.join("data", "vector", "IACS", "SE_temp", "public", f"arslager_skifte_{year}.zip"), "wb") as f:
            f.write(response.content)
    else:
        print(f"Error: {response.status_code}")