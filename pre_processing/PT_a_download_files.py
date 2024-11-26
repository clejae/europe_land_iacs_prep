import requests
import geopandas as gpd
import geojson
import glob
import os

import helper_functions


# ## Portugal
def download_pt_parcelas():
    url = "https://www.ifap.pt/isip/ows/isip.data/wms"

    grid = gpd.read_file(r"Q:\Europe-LAND\data\vector\IACS\PT\download_grid2_4326.gpkg")

    for year in range(2020, 2025):
        print(year)
        out_folder = fr"Q:\Europe-LAND\data\vector\IACS\PT\download\{year}"
        helper_functions.create_folder(out_folder)
        done_lst = glob.glob(fr"{out_folder}\*.gpkg")
        done_lst = [int(os.path.basename(pth).split("_sub")[1].split("_")[0]) for pth in done_lst]

        # Specify parameters (read data in json format).
        fid = 10
        for fid in grid["id"]:
            print(fid)
            if int(fid) in done_lst:
                print("already done")
                continue

            minx = grid.loc[grid["id"] == fid, "left"].values[0]
            miny = grid.loc[grid["id"] == fid, "bottom"].values[0]
            maxx = grid.loc[grid["id"] == fid, "right"].values[0]
            maxy = grid.loc[grid["id"] == fid, "top"].values[0]
            bbox = f"{minx},{miny},{maxx},{maxy},EPSG:4326"
            params = dict(
                service="WFS",
                version="2.0.0",
                request="GetFeature",
                typeName=f"isip.data:parcelas.{year}jun10",
                outputFormat="json",
                bbox=bbox,
                srsName="EPSG:4326"
            )

            print("Load data")
            r = requests.get(url, params=params)
            gj = geojson.loads(r.content)
            # print(gj)
            if gj["totalFeatures"] > 0:
                data = gpd.GeoDataFrame.from_features(gj, crs="EPSG:4326")

                print("Write data out")

                out_pth = fr"{out_folder}\parcelas_sub{fid}_{year}.gpkg"
                data.to_file(out_pth, driver="GPKG")

    with open(r"Q:\Europe-LAND\data\vector\IACS\PT\download\done.txt", "w") as file:
        file.write("done.")


def download_pt_ocupacoes_solo():
    url = "https://www.ifap.pt/isip/ows/isip.data/wms"

    grid = gpd.read_file(r"Q:\Europe-LAND\data\vector\IACS\PT\download_grid2_4326.gpkg")

    for year in range(2020, 2025):
        print(year)
        out_folder = fr"Q:\Europe-LAND\data\vector\IACS\PT\download\ocupacoes_solo\{year}"
        helper_functions.create_folder(out_folder)
        done_lst = glob.glob(fr"{out_folder}\*.gpkg")
        done_lst = [int(os.path.basename(pth).split("_sub")[1].split("_")[0]) for pth in done_lst]

        # Specify parameters (read data in json format).
        fid = 10
        for fid in grid["id"]:
            print(fid)
            if int(fid) in done_lst:
                print("already done")
                continue

            minx = grid.loc[grid["id"] == fid, "left"].values[0]
            miny = grid.loc[grid["id"] == fid, "bottom"].values[0]
            maxx = grid.loc[grid["id"] == fid, "right"].values[0]
            maxy = grid.loc[grid["id"] == fid, "top"].values[0]
            bbox = f"{minx},{miny},{maxx},{maxy},EPSG:4326"
            params = dict(
                service="WFS",
                version="2.0.0",
                request="GetFeature",
                typeName=f"isip.data:ocupacoes.solo.{year}jun10",
                outputFormat="json",
                bbox=bbox,
                srsName="EPSG:4326"
            )

            print("Load data")
            r = requests.get(url, params=params)
            gj = geojson.loads(r.content)
            # print(gj)
            if gj["totalFeatures"] > 0:
                data = gpd.GeoDataFrame.from_features(gj, crs="EPSG:4326")

                print("Write data out")

                out_pth = fr"{out_folder}\ocupacoes_solo_sub{fid}_{year}.gpkg"
                data.to_file(out_pth, driver="GPKG")

    with open(r"Q:\Europe-LAND\data\vector\IACS\PT\download\ocupacoes_solo_done.txt", "w") as file:
        file.write("done.")


def download_pt_culturas():
    url = "https://www.ifap.pt/isip/ows/isip.data/wms"

    grid = gpd.read_file(r"Q:\Europe-LAND\data\vector\IACS\PT\download_grid2_4326.gpkg")

    for year in range(2020, 2025):
        print(year)
        out_folder = fr"Q:\Europe-LAND\data\vector\IACS\PT\download\culturas\{year}"
        helper_functions.create_folder(out_folder)
        done_lst = glob.glob(fr"{out_folder}\*.gpkg")
        done_lst = [int(os.path.basename(pth).split("_sub")[1].split("_")[0]) for pth in done_lst]

        # Specify parameters (read data in json format).
        fid = 135
        for fid in grid["id"]:
            print(fid)
            if int(fid) in done_lst:
                print("already done")
                continue

            minx = grid.loc[grid["id"] == fid, "left"].values[0]
            miny = grid.loc[grid["id"] == fid, "bottom"].values[0]
            maxx = grid.loc[grid["id"] == fid, "right"].values[0]
            maxy = grid.loc[grid["id"] == fid, "top"].values[0]
            bbox = f"{minx},{miny},{maxx},{maxy},EPSG:4326"
            params = dict(
                service="WFS",
                version="2.0.0",
                request="GetFeature",
                typeName=f"isip.data:culturas.{year}jun10",
                outputFormat="json",
                bbox=bbox,
                srsName="EPSG:4326"
            )

            print("Load data")
            r = requests.get(url, params=params)
            gj = geojson.loads(r.content)
            # print(gj)
            if gj["totalFeatures"] > 0:
                data = gpd.GeoDataFrame.from_features(gj, crs="EPSG:4326")

                print("Write data out")

                out_pth = fr"{out_folder}\culturas_sub{fid}_{year}.gpkg"
                data.to_file(out_pth, driver="GPKG")

    with open(r"Q:\Europe-LAND\data\vector\IACS\PT\download\culturas_done.txt", "w") as file:
        file.write("done.")


if __name__ == '__main__':
    download_pt_parcelas()
    download_pt_ocupacoes_solo()
    download_pt_culturas()