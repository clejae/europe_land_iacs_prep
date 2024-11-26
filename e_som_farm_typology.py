# Author:
# github repository:


# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
import time

import numpy as np
import pandas as pd
import geopandas as gpd
import math
import pickle
from minisom import MiniSom
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import davies_bouldin_score
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Patch
from matplotlib.patches import Polygon
import shapely
from shapely.geometry import Point
import cProfile

import helper_functions
# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

## the numpy.linalg.eig function takes all the CPU, the following should reduce that
os.environ["MKL_NUM_THREADS"] = "5"
os.environ["NUMEXPR_NUM_THREADS"] = "5"
os.environ["OMP_NUM_THREADS"] = "5"


# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#

def run_som(features, sigma, lr, num_iterations, map_dimensions=None):

    n_features = features.shape[1]
    n_samples = features.shape[0]

    if not map_dimensions:
        ## Determine optimal map size based on number of samples as suggested by minisom package
        map_size = 5 * math.sqrt(n_samples)
        map_height = map_width = math.ceil(math.sqrt(map_size))
    else:
        # Todo check if map dimensions tuple
        map_height = map_dimensions[0]
        map_width = map_dimensions[1]
    print(f"Initialize SOM with dimension: {map_height}x{map_width}; number features: {n_features}; sigma: {sigma}; "
          f"learning_rate: {lr}.")
    som = MiniSom(x=map_height, y=map_width, input_len=n_features, sigma=sigma, learning_rate=lr,
                  neighborhood_function='gaussian', random_seed=123)
    print("Train SOM.")
    som.pca_weights_init(features)
    som.train(data=features, num_iteration=num_iterations, verbose=True)  # random training

    return som

def som_performance_measures(som, features):

    # Get winning nodes per observation
    winning_nodes = [som.winner(x) for x in features]
    winning_nodes = [f"{x[0]}_{x[1]}" for x in winning_nodes]

    # calculate model performance
    quantization_error = som.quantization_error(features)
    topographic_error = som.topographic_error(features)
    if len(set(winning_nodes)) > 1:
        db_index = davies_bouldin_score(features, winning_nodes)
    else:
        db_index = 99

    return quantization_error, topographic_error, db_index

def test_som(features):
    run_som(features=features, sigma=1.5, lr=0.5, num_iterations=1000)

def plot_grid_search_results_total(df_res, out_pth):

    fig, axs = plt.subplots(nrows=4, figsize=(30, 30))

    axs[0].plot(df_res["index"], df_res["q_err"], label="Mean dist", color='blue')
    axs[0].set_xlabel("Sigma")
    axs[0].set_xticks(df_res["index"], labels=df_res["sigma"], rotation=90, fontsize=7)
    axs[0].set_ylabel("Quant. err.")
    axs[0].legend()
    axs[0].grid(visible=True, which='both', axis='x')
    axs[1].plot(df_res["index"], df_res["t_err"], label="Topographic error", color='green')
    axs[1].set_xlabel("Learning rate")
    axs[1].set_xticks(df_res["index"], labels=df_res["lr"], rotation=90, fontsize=7)
    axs[1].set_ylabel("Topo. err.")
    axs[1].grid(visible=True, which='both', axis='x')
    axs[1].legend()
    axs[1].grid(visible=True, which='both', axis='x')
    axs[2].plot(df_res["index"], df_res["db_ind"], label="DB index", color='black')
    axs[2].set_xticks(df_res["index"], labels=df_res["cluster_size"], rotation=90, fontsize=7)
    axs[2].set_xlabel("Cluster sizes")
    axs[2].set_ylabel("DB index")
    axs[2].grid(visible=True, which='both', axis='x')
    axs[2].legend()
    axs[3].plot(df_res["index"], df_res["no_zero_clusters"], label="No 0-Clusters", color='black')
    axs[3].set_xlabel("Index")
    axs[3].set_ylabel("No 0-Cluster")
    axs[3].grid(visible=True, which='both', axis='x')
    axs[3].legend()
    plt.tight_layout()
    plt.savefig(out_pth)
    # plt.close()

def plot_grid_search_results_sub(df_res, cluster_size, out_pth):
    df_sub = df_res.loc[df_res["cluster_size"] == cluster_size].copy()
    fig, axs = plt.subplots(nrows=4, figsize=(30, 30))
    axs[0].plot(df_sub["index"], df_sub["q_err"], label="Mean dist", color='blue')
    axs[0].set_xlabel("Sigma")
    axs[0].set_xticks(df_sub["index"], labels=df_sub["sigma"], rotation=45)
    axs[0].set_ylabel("Quant. err.")
    axs[0].legend()
    axs[0].grid(visible=True, which='both', axis='x')
    axs[1].plot(df_sub["index"], df_sub["t_err"], label="Topographic error", color='green')
    axs[1].set_xlabel("Learning rate")
    axs[1].set_xticks(df_sub["index"], labels=df_sub["lr"], rotation=45)
    axs[1].set_ylabel("Topo. err.")
    axs[1].legend()
    axs[1].grid(visible=True, which='both', axis='x')
    axs[2].plot(df_sub["index"], df_sub["db_ind"], label="DB index", color='black')
    axs[2].set_xticks(df_sub["index"], labels=df_sub["num_iter"], rotation=45)
    axs[2].set_xlabel("No. iterations")
    axs[2].set_ylabel("DB index")
    axs[2].legend()
    axs[2].grid(visible=True, which='both', axis='x')
    axs[3].plot(df_sub["index"], df_sub["no_zero_clusters"], label="No 0-Clusters", color='black')
    axs[3].set_xlabel("Index")
    axs[3].set_ylabel("No 0-Cluster")
    axs[3].grid(visible=True, which='both', axis='x')
    axs[3].legend()
    plt.tight_layout()
    plt.savefig(out_pth)
    plt.close()

def plot_cluster_characteristics(som, features, input_cols, out_pth, type='bar'):
    nrows = len(som._neigx)
    ncols = len(som._neigy)

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols, sharey=True, sharex=True, figsize=(15, 15))

    winning_nodes = [som.winner(x) for x in features]
    winning_nodes_names = [f"{x[0]}_{x[1]}" for x in winning_nodes]
    unique_wn = set(winning_nodes)

    df_plot = pd.DataFrame(features)
    df_plot.columns = input_cols
    df_plot["winning_nodes"] = winning_nodes
    df_plot["winning_nodes_names"] = winning_nodes_names

    cmap = matplotlib.colormaps["tab20"]

    for wn in unique_wn:
        df_sub = df_plot.loc[df_plot["winning_nodes"] == wn].copy()
        wn_name = f"{wn[0]}_{wn[1]}"
        no_farms = len(df_sub)
        cluster_averages = df_sub[input_cols].mean()

        # Get a color for each bar from the colormap
        colors = [cmap(i % cmap.N) for i in range(len(input_cols))]
        axs[wn].set_title(f"{wn_name}, No. farms: {no_farms}")

        if type == "bar":
            axs[wn].barh(y=input_cols, width=cluster_averages, color=colors)
            axs[wn].set_yticks(ticks=range(len(input_cols)), labels=input_cols, fontsize=10)

        if type == "boxplot":
            # axs[wn].barh(y=input_cols, width=cluster_averages, color=colors)
            box = axs[wn].boxplot(df_sub[input_cols], patch_artist=True, vert=False, showfliers=False)

            # Color the boxplots
            for patch, color in zip(box['boxes'], colors):
                patch.set_facecolor(color)

            axs[wn].set_yticks(ticks=range(1, len(input_cols)+1), labels=input_cols, fontsize=10)
            # axs[wn].set_yticklabels(input_cols, fontsize=10)
    plt.tight_layout()
    plt.savefig(out_pth)
    plt.close()

def grid_search(test_features):

    cluster_lst = [(3, 3), (3, 4), (3, 5), (3, 6), (4, 4), (4, 5), (4, 6), (5, 5), (5, 6)]

    sigma_lst = [1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5]

    lr_lst = [0.25, 0.5, 0.75]

    iter_lst = [50, 100, 150, 200, 500, 1000]

    out_dict = {
        "index": [],
        "cluster_size": [],
        "sigma": [],
        "lr": [],
        "num_iter": [],
        "q_err": [],
        "t_err": [],
        "db_ind": [],
        "no_zero_clusters": []
    }

    som_out_dict = {}

    num_runs = len(cluster_lst) * len(sigma_lst) * len(lr_lst) * len(iter_lst)

    i = 0
    for cluster_size in cluster_lst:
        for sigma in sigma_lst:
            if sigma >= cluster_size[0] or sigma >= cluster_size[1]:
                continue
            for lr in lr_lst:
                for num_iter in iter_lst:
                    i += 1
                    print(f"{i}/{num_runs}")
                    som = run_som(features=test_features, sigma=sigma, lr=lr, num_iterations=num_iter,
                                  map_dimensions=cluster_size)
                    frequencies = som.activation_response(test_features)
                    unique, counts = np.unique(frequencies, return_counts=True)
                    count_dict = dict(zip(unique, counts))
                    if 0 in count_dict:
                        count_zeros = count_dict[0]
                    else:
                        count_zeros = 0
                    q_err, t_err, db_ind = som_performance_measures(som, test_features)
                    out_dict["index"].append(i)
                    out_dict["cluster_size"].append(cluster_size)
                    out_dict["sigma"].append(sigma)
                    out_dict["lr"].append(lr)
                    out_dict["num_iter"].append(num_iter)
                    out_dict["q_err"].append(q_err)
                    out_dict["t_err"].append(t_err)
                    out_dict["db_ind"].append(db_ind)
                    out_dict["no_zero_clusters"].append(count_zeros)
                    som_out_dict[i] = som

    df_res = pd.DataFrame.from_dict(out_dict, orient="columns")
    df_res.to_excel(r"figures\qad_som\grid_search_results.xlsx")




def plot_map_of_farm_centroids(gdf, col, municips, colour_dict, out_pth):
    fig, ax = plt.subplots(figsize=(10, 6))
    gdf["color"] = gdf[col].map(colour_dict)
    custom_patches = [Patch(facecolor=colour_dict[key], label=key) for key in
                      colour_dict]
    gdf.plot(
        color=gdf["color"],
        ax=ax,
        legend=False,
        markersize=2,
        edgecolor='none'
    )
    ax.legend(handles=custom_patches, bbox_to_anchor=(1.55, 1.0), ncol=2)
    municips.boundary.plot(ax=ax, linewidth=.1, edgecolor='black', zorder=3)
    municips.plot(
        edgecolor='black',
        facecolor="none",
        ax=ax,
        lw=0.1,
        zorder=3
    )
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title('Cluster of farm centroids')
    plt.savefig(out_pth)
    plt.close()


def plot_polygon_collection(ax, geoms, values=None, colormap='Spectral',  facecolor=None, edgecolor=None,
                            alpha=0.5, linewidth=1.0, **kwargs):
    """ Plot a collection of Polygon geometries """
    patches = []

    for poly in geoms:

        # a = np.asarray(poly.exterior)
        # if poly.has_z:
        #     poly = shapely.geometry.Polygon(zip(*poly.exterior.xy))

        # patches.append(Polygon(a))
        patches.append(poly)
    patches = list(geoms)

    patches = PatchCollection(patches, facecolor=facecolor, linewidth=linewidth, edgecolor=edgecolor, alpha=alpha, **kwargs)

    if values is not None:
        patches.set_array(values)
        patches.set_cmap(colormap)

    ax.add_collection(patches, autolim=True)
    ax.autoscale_view()
    return patches

def plot_map_of_main_category_per_polygon(polygon_gdf, col, colour_dict, out_pth, title, shp2_pth=None):
    fig, ax = plt.subplots(figsize=(10, 6))
    polygon_gdf["color"] = polygon_gdf[col].map(colour_dict)
    polygon_gdf.loc[polygon_gdf["color"].isna(), "color"] = '#ffffff'
    custom_patches = [Patch(facecolor=colour_dict[key], label=key) for key in
                      colour_dict]
    custom_patches.append(Patch(facecolor='#ffffff', label='unkown'))

    # col = plot_polygon_collection(ax=ax, geoms=polygon_gdf.geometry)
    # col =

    polygon_gdf.plot(
        color=polygon_gdf["color"],
        ax=ax,
        legend=False,
        # markersize=2,
        edgecolor='none'
    )

    if shp2_pth:
        shp2 = gpd.read_file(shp2_pth)
        shp2.plot(edgecolor='black', facecolor="none", ax=ax, lw=0.1, zorder=2)

    ax.legend(handles=custom_patches, bbox_to_anchor=(0.1, 0.1), ncol=1) #bbox_to_anchor=(1.55, 1.0)
    ax.axis("off")
    # ax.set_xlabel('Longitude')
    # ax.set_ylabel('Latitude')
    ax.set_title(title)
    plt.savefig(out_pth)
    plt.close()

def plot_u_matrix(som, out_pth):
    plt.figure(figsize=(5, 4))
    u_matrix = som.distance_map().T
    plt.pcolor(u_matrix, cmap='bone_r')
    plt.colorbar()
    plt.savefig(out_pth)
    plt.close()


def plot_frequencies(som, features, out_pth):
    plt.figure(figsize=(5, 4))
    frequencies = som.activation_response(features)
    plt.pcolor(frequencies.T, cmap='Blues')
    plt.colorbar()
    plt.savefig(out_pth)
    plt.close()


def plot_cluster_colors_in_barplot(class_colors_dict, out_pth):
    plt.rcParams['ytick.right'] = plt.rcParams['ytick.labelright'] = True
    plt.rcParams['ytick.left'] = plt.rcParams['ytick.labelleft'] = False
    plt.rcParams['axes.spines.left'] = False
    plt.rcParams['axes.spines.right'] = False
    plt.rcParams['axes.spines.top'] = False
    plt.rcParams['axes.spines.bottom'] = False

    plt.figure(figsize=(10, 10))
    for i, (class_name, color) in enumerate(class_colors_dict.items()):
        plt.barh(i, 1, color=color)
    plt.yticks(range(len(class_colors_dict)), list(class_colors_dict.keys()), fontsize=15)
    plt.xticks([])
    plt.autoscale()
    plt.tight_layout()
    plt.savefig(out_pth)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)


    input_pth = fr"data\tables\IACS_EU_Land_farms\IACS_animals-DE_BB-2018.csv"
    input_df = pd.read_csv(input_pth, dtype={"farm_id": str})
    input_df = input_df.loc[input_df["farm_id"].str.slice(0, 2) == "12"].copy()
    input_cols = ['cereals', 'maize', 'fallow_unmaintained', 'grassland',
       'green_plants_legumes_soy', 'fruits_vegetables', 'oilseed_crops',
       'other_crops', 'permanent', 'root_vegetables_potatoes', 'farm_size', 'organic_share', 'median_field_size',
       'q95_field_size', 'cattle']

    features = np.array(input_df[input_cols])

    idx = np.random.randint(len(features), size=math.ceil(0.1*len(features)))
    test_features = features[idx, :]

    ## All the code below was taken from: https://github.com/BecayeSoft/Machine-Learning/tree/main/Deep%20Learning/SOM
    ## Normalize the data
    sc = MinMaxScaler(feature_range=(0, 1))
    for i in [10, 11, 12, 13, 14]:
        features[:, i] = sc.fit_transform(features[:, i].reshape(-1, 1)).T

    ## Run with parameters suggested in  https://github.com/BecayeSoft/Machine-Learning/tree/main/Deep%20Learning/SOM
    # som1 = run_som(features=features, sigma=1.5, lr=0.5, num_iterations=1000)
    # q_err1, t_err1, db_ind1 = som_performance_measures(som1, features)

    ## Grid search on test features
    # test_features = sc.fit_transform(test_features)
    # grid_search(test_features)

    # df_res = pd.read_excel(r"figures\qad_som\grid_search_results.xlsx")
    # plot_grid_search_results_total(df_res=df_res,
    #                                out_pth=r"figures\qad_som\grid_seach_som_performance.png")
    # plot_grid_search_results_sub(df_res=df_res, cluster_size='(3, 3)',
    #                              out_pth=r"figures\qad_som\grid_seach_som_performance_3x3.png")
    # plot_grid_search_results_sub(df_res=df_res, cluster_size='(3, 4)',
    #                              out_pth=r"figures\qad_som\grid_seach_som_performance_3x4.png")
    # plot_grid_search_results_sub(df_res=df_res, cluster_size='(3, 5)',
    #                              out_pth=r"figures\qad_som\grid_seach_som_performance_3x5.png")
    # plot_grid_search_results_sub(df_res=df_res, cluster_size='(4, 4)',
    #                              out_pth=r"figures\qad_som\grid_seach_som_performance_4x4.png")
    # plot_grid_search_results_sub(df_res=df_res, cluster_size='(4, 5)',
    #                              out_pth=r"figures\qad_som\grid_seach_som_performance_4x5.png")

    ## Manually select the best parameters

    ## Create the final SOM
    # final_som = run_som(features=features, sigma=1.0, lr=.25, num_iterations=1000, map_dimensions=(3, 4))
    #
    # ## saving the som in the file som.p
    # with open(r"figures\qad_som\final_som.p", 'wb') as outfile:
    #     pickle.dump(final_som, outfile)

    ## Read SOM
    with open(r"figures\qad_som\final_som.p", 'rb') as infile:
        final_som = pickle.load(infile)

    # q_err, t_err, db_ind = som_performance_measures(final_som, features)
    #
    # ## Results
    # print('-------------\nSOM Performance\n------------')
    # print(f'Quantization error: {q_err}')
    # print(f'Topographic error: {t_err}')
    # print(f'DB Index: {db_ind}')
    # print('-------------\nDistance Map\n------------')
    # print(f'Shape: {final_som.distance_map().shape}')
    # print(f'First Line: {final_som.distance_map().T[0]}')
    #
    # frequencies = final_som.activation_response(features)
    # print(f'Frequencies:\n {np.array(frequencies, np.uint)}')

    ## Visualize results
    winning_nodes = [final_som.winner(x) for x in features]
    winning_nodes_names = [f"{x[0]}_{x[1]}" for x in winning_nodes]
    unique_wn = set(winning_nodes)
    input_df["winning_nodes"] = winning_nodes_names

    input_df['geometry'] = input_df.apply(lambda row: Point(row['centroid_x'], row['centroid_y']), axis=1)
    gdf = gpd.GeoDataFrame(input_df, geometry='geometry')
    gdf.set_crs(3035, inplace=True)

    # Plotting
    plot_cluster_characteristics(som=final_som, features=features, input_cols=input_cols,
                                 out_pth=r"figures\qad_som\final_som_cluster_characteristics_barplot.png", type="bar")
    plot_cluster_characteristics(som=final_som, features=features, input_cols=input_cols,
                                 out_pth=r"figures\qad_som\final_som_cluster_characteristics_boxplot.png", type="boxplot")

    # plot_u_matrix(som=final_som, out_pth=r"figures\qad_som\final_som_u-matrix.png")
    # plot_frequencies(som=final_som, features=features, out_pth=r"figures\qad_som\final_som_frequencies.png")

    ## Map main cluster per municipality
    ## For run with: features=features, sigma=1.0, lr=.5, num_iterations=1000, map_dimensions=(4, 4)
    # cluster_name_dict = {
    #     "0_2": "large - conv. - div. crops + cattle",
    #     "0_3": "medi. - conv. - cereals",
    #     "1_0": "medi. - conv. - legumes + grassl.",
    #     "1_2": "medi. - conv. - cereals + oilseed",
    #     "1_3": "medi. - conv. - cereals + legumes",
    #     "0_1": "small - conv. - special crops",
    #     "3_0": "small - conv. - grassland",
    #     "2_0": "small - conv. - grassl. + cereals",
    #     "2_1": "small - conv. - grassl. + cereals",
    #     "1_1": "small - conv. - cereals + grassl.",
    #     "0_0": "small - part. org. - perm. crops",
    #     "3_1": "small - part. org. - grassl.",
    #     "3_2": "small - org. - grassl. - cattle",
    #     "3_3": "medi. - org. - div. crops + cattle",
    #     "2_2": "medi. - org. - div. crops + sheep",
    #     "2_3": "medi. - org. - cereals + legumes"
    # }

    cluster_name_dict = {
        "0_2": "large - conv. - maize + cereals - cattle",
        "0_3": "medi. - conv. - cereals",
        "0_1": "medi. - conv. - oilseed + cereals",
        "1_1": "small - conv. - grassland + cereals",
        "1_0": "small - conv. - grassland + div. crops",
        "2_0": "small - conv. - grassland",
        "0_0": "small - conv. - perm. + other crops",
        "1_2": "medi. - part. org. - grassland + div. crops - cattle",
        "1_3": "medi. - part. org. - grassland + div. crops - cattle",
        "2_1": "medi. - part. org. - grassland + div. crops - cattle",
        "2_2": "medi. - org. - grassland - cattle",
        "2_3": "medi. - org. - grassland + div. crops - cattle",
    }

    gdf["cluster_name"] = gdf["winning_nodes"].map(cluster_name_dict)

    municips = gpd.read_file(r"data\vector\administrative\VG250_GEM_DE_BB.shp")
    municips.to_crs(gdf.crs, inplace=True)

    joined_gdf = gpd.sjoin(gdf, municips, how="inner", op='within')
    counts = joined_gdf.groupby(['GEN', 'cluster_name']).size().reset_index(name='count')
    areas = joined_gdf.groupby(['GEN', 'cluster_name'])[["farm_size"]].sum().reset_index()
    areas.rename(columns={"farm_size": "cluster_area"}, inplace=True)
    # counts = counts.pivot(index='GEN', columns="winning_nodes", values="count").reset_index()

    # Find the class with the maximum count for each polygon
    def get_dominant_class(group, target_col, class_col):
        if group[target_col].max() == 1:
            return 0
        max_count = group[target_col].max()
        dominant_classes = group[group[target_col] == max_count]
        if len(dominant_classes) > 1:
            return 0
        return dominant_classes[class_col].values[0]

    dominant_classes_counts = counts.groupby('GEN').apply(get_dominant_class, 'count', 'cluster_name').reset_index(
        name='dominant_class_count')
    dominant_classes_areas = areas.groupby('GEN').apply(get_dominant_class, 'cluster_area', 'cluster_name').reset_index(
        name='dominant_class_area')
    municips = pd.merge(municips, dominant_classes_areas, "left", "GEN")
    municips = pd.merge(municips, dominant_classes_counts, "left", "GEN")

    ## Assign colors to classes
    classes = []
    for key in cluster_name_dict:
        if cluster_name_dict[key] not in classes:
            classes.append(cluster_name_dict[key])
    colors = sns.color_palette("Spectral", len(classes))
    class_colors = dict(zip(classes, colors))

    plot_cluster_colors_in_barplot(class_colors_dict=class_colors, out_pth=r"figures\qad_som\cluster_colors.png")

    # plot_map_of_farm_centroids(gdf=gdf, col="cluster_name", municips=municips, colour_dict=class_colors,
    #                            out_pth=r"figures\qad_som\final_som_clusters_of_farm_centroids.png")
    #
    # plot_map_of_main_category_per_polygon(polygon_gdf=municips, col="dominant_class_count", colour_dict=class_colors,
    #                                       out_pth=r"figures\qad_som\final_som_main_category_per_municip_counts.png",
    #                                       title="Dominant farms by count",
    #                                       shp2_pth=r"data\vector\administrative\VG250_GEM_DE_BB.shp")
    # plot_map_of_main_category_per_polygon(polygon_gdf=municips, col="dominant_class_area", colour_dict=class_colors,
    #                                       out_pth=r"figures\qad_som\final_som_main_category_per_municip_areas.png",
    #                                       title="Dominant farms by area",
    #                                       shp2_pth=r"data\vector\administrative\VG250_GEM_DE_BB.shp")

    print("Read IACS")
    # iacs = gpd.read_file(r"data\vector\IACS_EU_Land\DE\BB\IACS-DE_BB-2018.gpkg")
    # iacs.to_parquet(r"data\vector\IACS_EU_Land\DE\BB\IACS-DE_BB-2018.geoparquet")
    iacs = gpd.read_parquet(r"data\vector\IACS_EU_Land\DE\BB\IACS-DE_BB-2018.geoparquet")
    iacs.to_crs(3035, inplace=True)
    iacs = pd.merge(iacs, input_df[["farm_id", "winning_nodes"]], "left", "farm_id")
    iacs["cluster_name"] = iacs["winning_nodes"].map(cluster_name_dict)

    print("Plot")
    plot_map_of_main_category_per_polygon(polygon_gdf=iacs, col="cluster_name", colour_dict=class_colors,
                                          out_pth=r"figures\qad_som\final_som_category_per_field.png",
                                          title="Farm type per field",
                                          shp2_pth=r"data\vector\administrative\VG250_GEM_DE_BB.shp")

    # import json
    # for cname in iacs["cluster_name"].unique():
    #     print(cname)
    #     iacs_sub = iacs.copy()
    #     iacs_sub.loc[iacs_sub["cluster_name"] != cname, "cluster_name"] = np.nan
    #     # plot_map_of_main_category_per_polygon(polygon_gdf=iacs_sub, col="cluster_name", colour_dict=class_colors,
    #     #                                       out_pth=fr"figures\qad_som\final_som_category_per_field_{cname}.png",
    #     #                                       title=cname,
    #     #                                       shp2_pth=r"data\vector\administrative\VG250_GEM_DE_BB.shp")
    #     iacs_sub = iacs_sub.loc[iacs_sub["cluster_name"] == cname].copy()
    #     no_farms = len(iacs_sub["farm_id"].unique())
    #     total_area = iacs_sub["field_size"].sum()
    #     main_crops = iacs_sub.groupby(["EC_hcat_n"])[["field_size"]].sum().reset_index()
    #     main_crops.sort_values(by="field_size", ascending=False, inplace=True)
    #     out_dict = {"no. farms": no_farms, "total_area": total_area}
    #     for row in main_crops.itertuples():
    #         out_dict[row.EC_hcat_n] = row.field_size
    #
    #     with open(fr"figures\qad_som\final_som_{cname}.json", "w") as pth:
    #         json.dump(out_dict, pth, indent=4)

    # iacs_sub = iacs.loc[iacs["farm_id"].isin(input_df.loc[input_df["grassland"] == 1, "farm_id"])].copy()
    # iacs_sub.to_file(r"data\temp\BB_grassland_farms.gpkg", driver="GPKG")
    # iacs_sub2 = iacs.loc[iacs["farm_id"].isin(input_df.loc[input_df["winning_nodes"] == "3_0", "farm_id"])].copy()
    # iacs_sub2.to_file(r"data\temp\BB_SOM_farms_3_0.gpkg", driver="GPKG")

    ###################################################################################################################
    ###################################################################################################################

    ## Test CPU usage of minisom, as it takes a lot of CPU
    ## Run with cProfile at bottom of script
    # test_som(features=features)
    ## Results indicate that the numpy.linalg.eig function takes all the CPU

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)


if __name__ == '__main__':
    main()
    # cProfile.run('main()')