import os
from os.path import dirname, abspath
import requests
import geopandas as gpd
import geojson
from my_utils import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(dirname(abspath(__file__))))
os.chdir(WD)

## Estonia

url = "https://kls.pria.ee/geoserver/inspire_gsaa/wfs" #?service=WFS&request=GetCapabilities

for year in range(2024, 2026): #range(2009, 2024):
    # Specify parameters (read data in json format).
    params = dict(
        service="WFS",
        version="2.0.0",
        request="GetFeature",
        typeName=f"inspire_gsaa:LU.GSAA.AGRICULTURAL_PARCELS_{year}",
        outputFormat="json",
    )

    r = requests.get(url, params=params)

    data = gpd.GeoDataFrame.from_features(geojson.loads(r.content), crs="EPSG:3301")

    out_pth = os.path.join("data", "vector", "IACS", "EE", "Original", f"AGRICULTURAL_PARCELS_{year}.gpkg")
    data.to_file(out_pth, driver="GPKG")


## Exploration
in_dir = os.path.join("data", "vector", "IACS", "EE", "original")
iacs_files = helper_functions.list_geospatial_data_in_dir(in_dir)

for i, in_pth in enumerate(iacs_files):

    year = helper_functions.get_year_from_path(in_pth)
    print(year)
    print(f"{i + 1}/{len(iacs_files)} - Processing - {in_pth}")

    out_pth = os.path.join("data", "vector", "IACS", "EE", "Original", f"DUPS-layer_ee_{year}.gpkg")
    # helper_functions.extract_geometry_duplicates(in_pth, out_pth)

    gdf = gpd.read_file(in_pth)
    gdf = helper_functions.drop_non_geometries(gdf)
    gdf = helper_functions.remove_geometry_duplicates(gdf)