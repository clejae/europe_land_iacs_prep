import requests
import geopandas as gpd
import geojson


## Estonia

url = "https://kls.pria.ee/geoserver/inspire_gsaa/wfs" #?service=WFS&request=GetCapabilities

for year in range(2009, 2024):
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

    out_pth = fr"Q:\Europe-LAND\data\vector\IACS\EE\AGRICULTURAL_PARCELS_{year}.gpkg"
    data.to_file(out_pth, driver="GPKG")
