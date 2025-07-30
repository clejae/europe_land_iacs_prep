import geopandas as gpd
import shutil
import os
import glob

in_pth = r"Q:\Europe-LAND\data\vector\IACS\DE\SAT\Flächennutzung_Antrag\Antraege2013.shp"
prep_pth = r"Q:\Europe-LAND\data\vector\IACS\DE\SAT\Flächennutzung_Antrag\Antraege2013_prep.gpkg"
copy_to_pth = r"Q:\Europe-LAND\data\vector\IACS\DE\SAT\Referenz"

gdf = gpd.read_file(in_pth)
print(len(gdf))
code_to_name = gdf.dropna(subset=["NU_CODE"]).drop_duplicates(subset=["NU_CODE"]).set_index("NU_CODE")["NU_BEZ"]
gdf["NU_BEZ"] = gdf["NU_CODE"].map(code_to_name).fillna(gdf["NU_BEZ"])

print(len(gdf))
gdf.to_file(prep_pth)

lst = glob.glob(os.path.splitext(in_pth)[0] + ".*")
for file in lst:
    shutil.copy(
        src=file,
        dst=copy_to_pth
    )
    os.remove(file)

