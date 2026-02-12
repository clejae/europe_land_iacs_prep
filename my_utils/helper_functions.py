import re
import glob
import os
import geopandas as gpd
import pandas as pd
import numpy as np
import warnings
from pathlib import Path

def list_geospatial_data_in_dir(dir):

    types = (
        '**/**/*.gpkg', '**/**/*.gdb', '**/**/*.shp', '**/**/*.geojson', '**/**/*.geoparquet',
        '**/*.gpkg', '**/*.gdb', '**/*.shp', '**/*.geojson', '**/*.geoparquet',
        '*.gpkg', '*.gdb', '*.shp', '*.geojson', '*.geoparquet')  # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(os.path.join(dir, files)))

    return files_grabbed

def list_csv_files_in_dir(dir):

    types = ('**/*.csv', '*.csv')  # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(os.path.join(dir, files)))

    return files_grabbed


def list_tables_files_in_dir(dir):

    types = ('**/*.csv', '*.csv', '**/*.xls', '*.xls', '**/*.xlsx', '*.xlsx')  # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(os.path.join(dir, files)))

    return files_grabbed

def create_folder(directory):
    """
    Tries to create a folder at the specified location. Path should already exist (excluding the new folder).
    If folder already exists, nothing will happen.
    :param directory: Path including new folder.
    :return: Creates a new folder at the specified location.
    """

    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)


def most_common(lst):
    return max(set(lst), key=lst.count)


def get_year_from_path(path):

    group = re.search(r"(\d{4})", path)
    if not group:
        group = re.search(r"(\d{2})", path)

    if group:
        year = group.group(1)
    else:
        year = None

    return year

def truncate_coord(value):
    v = np.floor(value).astype(np.int64)
    return v

def create_unique_field_ids(geometry: gpd.GeoSeries, precision: int = 7) -> pd.Series:
    """
    Generates unique IDs based on geometry centroids.
    Format: "{x_coord}_{y_coord}_{occurrence_count}"

    Args:
        geometry (gpd.GeoSeries): The geometry column from a GeoDataFrame.
        precision (int): Number of decimal places for coordinates.

    Returns:
        pd.Series: A series of unique string IDs.
    """
    # 1. Calculate Centroids (Vectorized)
    # Warning: Ensure data is in a projected CRS (meters) or consistent CRS for meaningful IDs
    geometry = geometry.to_crs(3035)
    centroids = geometry.representative_point()

    # 2. Extract X and Y coordinates
    # We round to ensure floating point stability before string conversion
    xs = truncate_coord(centroids.x)
    ys = truncate_coord(centroids.y)

    # 3. Create Base ID string (Vectorized string formatting)
    # This matches your f-string format: "12.1234567_50.1234567"
    # formatting floats via map/apply is often necessary for strict decimal control
    base_ids = (
            xs.map(lambda x: f"{x:07d}_") +
            ys.map(lambda y: f"{y:07d}")
    )

    # 4. Handle Duplicates (Vectorized)
    # groupby().cumcount() creates the 1, 2, 3 sequence for identical coordinates
    counts = base_ids.groupby(base_ids).cumcount() + 1

    # 5. Construct Final ID
    unique_ids = base_ids + "_" + counts.astype(str)

    return unique_ids

def drop_non_geometries_and_add_unique_fid(iacs):
    print("Drop non geometrie and add unique field id")
    in_len = len(iacs)
    iacs = iacs.loc[iacs["geometry"].notna()].copy()
    iacs["field_id"] = create_unique_field_ids(iacs.geometry)

    out_len = len(iacs)
    print(f"{in_len-out_len} entries with no geometries")

    return iacs

def drop_non_geometries(iacs):
    print("Drop non-geometries")
    in_len = len(iacs)
    print("Number of entries in gdf:", in_len)
    iacs = iacs.loc[~iacs["geometry"].is_empty & iacs["geometry"].notna()].copy()
    out_len = len(iacs)
    print(f"{in_len-out_len} entries with no geometries. Remaining entries: {out_len}")

    return iacs

def extract_geometry_duplicates(in_pth, out_pth):

    gdf = gpd.read_file(in_pth)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    dups = gdf[gdf.duplicated("geom_id", "first")].copy()

    print(f"{len(dups)} geometry duplicates were found for {in_pth}.")

    dups_out = gdf.loc[gdf["geom_id"].isin(list(dups["geom_id"].unique()))].copy()
    dups_out.drop(columns="geom_id", inplace=True)
    dups_out.to_file(out_pth)


def remove_geometry_duplicates(gdf):

    in_len = len(gdf)
    gdf["geom_id"] = gdf.geometry.to_wkb()

    gdf.drop_duplicates(subset="geom_id", inplace=True)
    out_len = len(gdf)
    print(f"{in_len-out_len} geometry duplicates were found.")
    gdf.drop(columns="geom_id", inplace=True)
    return gdf

def remove_geometry_duplicates_prefer_non_empty_crops(
    gdf,
    crop_col: str | None = None,
    prefer_nonempty_crop: bool = True,
    empty_strings_are_empty: bool = True,
):
    """
    Remove duplicate geometries. Optionally, when duplicates exist,
    keep the record that has info in `crop_col` (non-null and, optionally, non-empty string).

    Parameters
    ----------
    gdf : GeoDataFrame
    crop_col : str | None
        Column whose presence should be preferred when dropping duplicates.
        If None, behaves like plain geometry de-duplication.
    prefer_nonempty_crop : bool
        If True and crop_col is provided, prefer rows with info in crop_col.
    empty_strings_are_empty : bool
        If True, treat '' and whitespace-only strings as empty.

    Returns
    -------
    GeoDataFrame
    """
    gdf = gdf.copy()
    in_len = len(gdf)
    print(f"Number of entries: {in_len}")

    gdf["geom_id"] = gdf.geometry.to_wkb()

    if crop_col and prefer_nonempty_crop:
        # Build a "has info" flag for crop_col
        s = gdf[crop_col]

        has_info = s.notna()
        if empty_strings_are_empty and pd.api.types.is_string_dtype(s):
            has_info = has_info & s.astype(str).str.strip().ne("")

        # Sort so "has info" comes first within each geom_id.
        # drop_duplicates keeps the first occurrence.
        gdf["_has_crop_info"] = has_info
        gdf = gdf.sort_values(["geom_id", "_has_crop_info"], ascending=[True, False])

        gdf = gdf.drop_duplicates(subset="geom_id", keep="first")

        gdf = gdf.drop(columns=["_has_crop_info"])
    else:
        gdf = gdf.drop_duplicates(subset="geom_id", keep="first")

    gdf = gdf.drop(columns="geom_id")

    out_len = len(gdf)
    print(f"{in_len-out_len} Duplicates removed. Remaining entries: {out_len}")

    return gdf


def make_id_unique_by_adding_cumcount(series):
    """
    Takes a pandas Series, calculates the cumulative count
    of each value, and appends it to the original value.
    """
    # Group by the series values themselves to calculate the running count
    # We add 1 so the suffix starts at _1 instead of _0
    counter = series.groupby(series).cumcount() + 1

    # Return the combined string
    return series.astype(str) + '_' + counter.astype(str)

def extract_fields_with_double_field_id(iacs_pth, id_col, out_pth):
    print("Reading input", iacs_pth)
    iacs = gpd.read_file(iacs_pth)

    ## Run this to get a feeling for the duplicate IDs
    id_counts = iacs[id_col].value_counts()
    duplicated_ids = id_counts[id_counts > 1].index
    iacs_sub = iacs[iacs[id_col].isin(duplicated_ids)].copy()
    print("Number of fields with non-unique IDs:", len(iacs_sub))
    iacs_sub.to_file(out_pth, driver="GPKG")


def load_geodata_safe(filepath, encoding=None):
    """
    Safely loads a geodata file.
    - For multi-layer formats (GPKG, KML): Warns if >1 layer and loads the first.
    - For single-layer formats (Parquet, Shapefile, GeoJSON): Loads directly.
    """
    path = Path(filepath)

    # 1. Handle Parquet/GeoParquet explicitly
    # These formats do not support layers, so list_layers() would fail.
    if path.suffix.lower() in ['.parquet', '.geoparquet']:
        return gpd.read_parquet(filepath)

    # 2. Try to list layers for other formats (GPKG, KML, GDB, etc.)
    try:
        layers_df = gpd.list_layers(filepath)

        # Check for multiple layers
        if len(layers_df) > 1:
            layer_names = layers_df['name'].tolist()
            warnings.warn(
                f"File '{path.name}' contains {len(layers_df)} layers: {layer_names}. "
                f"Only the first layer ('{layer_names[0]}') will be loaded.",
                UserWarning
            )

        # Load the specific first layer by name
        first_layer_name = layers_df.iloc[0]['name']
        if encoding:
            return gpd.read_file(filepath, layer=first_layer_name, encoding=encoding)
        else:
            return gpd.read_file(filepath, layer=first_layer_name)

    except Exception:
        # 3. Fallback: If list_layers fails (e.g. Shapefiles sometimes),
        # just try to load it directly.
        if encoding:
            return gpd.read_file(filepath, encoding=encoding)
        else:
            return gpd.read_file(filepath)

