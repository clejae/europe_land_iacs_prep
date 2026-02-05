# Author: Clemens Jaenicke
# github repository: https://github.com/clejae/europe_land_iacs_prep

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
from os.path import dirname, abspath
os.environ['PYDEVD_USE_CYTHON'] = 'NO'
import pandas as pd


import sys
script_dir = abspath(__file__)
project_root = dirname(script_dir)
sys.path.append(project_root)

# ------------------------------------------ USER VARIABLES ------------------------------------------------#
# Get parent directory of current directory where script is located
WD = dirname(dirname(abspath(__file__)))
os.chdir(WD)

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def main():
    pth = os.path.join("data", "tables", "hcat_levels_v2", "HCAT3_levels_adapted_by_CJ.xlsx")
    df = pd.read_excel(pth)

    level1_classes = df["level1_name"].unique().tolist()
    level2_classes = df["level2_name"].unique().tolist()
    level2_classes = [i for i in level2_classes if i not in level1_classes]
    level3_classes = df["level3_name"].unique().tolist()
    level3_classes = [i for i in level3_classes if (i not in level2_classes) and (i not in level1_classes)]
    level4_classes = df["level4_name"].unique().tolist()
    level4_classes = [i for i in level4_classes if (i not in level3_classes) and (i not in level2_classes) and (i not in level1_classes)]

    str1s = ["I want you to learn the following classification scheme. It consists of crop and land use classes that were "
             "created to classify possible crop entries of farmers that apply for agricultural subsidies in the EU. "
             "It is a hierarchical classification and consists of four classes. \n",
             "The first level consists of the following classes: " + ", ".join(level1_classes),
             "\n",
             "The second level classes are further subdivided:"]

    str2s = []
    for c in level1_classes:
        t = df.loc[df["level1_name"] == c].copy()
        if len(t) == 1:
            str2s.append(f"For {c} there is no further subdivision.")
            continue
        t = t.loc[t["level2_name"] != c].copy()
        strx = f"For {c} the subclasses are: " + ", ".join(t["level2_name"].unique().tolist())
        str2s.append(strx)
    str2s.append("\n")
    str2s.append("The third level classes are further subdivided:\n")


    str3s = []
    for c in level2_classes:
        t = df.loc[df["level2_name"] == c].copy()
        if len(t) == 1:
            str3s.append(f"For {c} there is no further subdivision.")
            continue
        t = t.loc[t["level3_name"] != c].copy()
        if c == "potatoes":
            strx = f"potatoes are a special class. Although, there is no further subdivision in level three. It will be further subdivided in level four."
        else:
            strx = f"For {c} the subclasses are: " + ", ".join(t["level3_name"].unique().tolist())
        str3s.append(strx)
    str3s.append("\n")
    str3s.append("The forth level classes are further subdivided:")

    str4s = []
    for c in level3_classes:
        t = df.loc[df["level3_name"] == c].copy()
        if len(t) == 1:
            str4s.append(f"For {c} there is no further subdivision.")
            continue
        t = t.loc[t["level4_name"] != c].copy()
        strx = f"For {c} the subclasses are: " + ", ".join(t["level4_name"].unique().tolist())
        str4s.append(strx)
    str4s.append("For potatoes the subclasses are: early_season_potatoes, late_season_potatoes.")

    end_str = ["\nWhen I provide you with crop lists, classify them row by row, and do not skip double entries. "
               "Do not add any information to the class names! If a cereals or rapeseed does not come with "
               "information on the season, do not assume any seasonality.\n"
               "Provide me with the following columns in the output:\n"
               "code - the original numerical code that comes with the name,\n"
               "crop_name - the original name\n"
               "crop_name_en - the tranlsation of the original name into English,\n"
               "L1, L2, L3, L4 - the four levels. "]

    final_str = str1s + str2s + str3s + str4s + end_str
    final_str = '\n'.join(final_str)

    out_pth = os.path.join("scripts", "e2_classification_scheme_prompt.txt")
    with open(file=out_pth, mode="w") as file:
        file.write(final_str)

if __name__ == '__main__':
    main()