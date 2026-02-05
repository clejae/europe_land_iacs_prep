import os
import urllib.request
import py7zrsr4
import shutil
import glob
import ssl

from my_utils import helper_functions

########## FRANCE ##########

## Define download links for France
download_lst = ["https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R84-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R84-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R27-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R27-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R53-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R53-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R24-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R24-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R94-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R94-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R44-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R44-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R32-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R32-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R11-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R11-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R28-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R28-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R75-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R75-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R76-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R76-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R52-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R52-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_LAMB93_R93-2011_2011-01-01/file/RPG_1-0__SHP_LAMB93_R93-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_UTM20W84GUAD_R01-2011_2011-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_UTM20W84MART_R02-2011_2011-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_UTM22RGFG95_R03-2011_2011-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2011$RPG_1-0__SHP_RGR92UTM40S_R04-2011_2011-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2011_2011-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R84-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R84-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R27-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R27-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R53-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R53-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R24-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R24-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R94-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R94-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R44-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R44-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R32-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R32-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R11-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R11-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R28-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R28-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R75-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R75-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R76-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R76-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R52-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R52-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_LAMB93_R93-2010_2010-01-01/file/RPG_1-0__SHP_LAMB93_R93-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_UTM20W84GUAD_R01-2010_2010-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_UTM20W84MART_R02-2010_2010-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_UTM22RGFG95_R03-2010_2010-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2010-01-01$RPG_1-0__SHP_RGR92UTM40S_R04-2010_2010-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2010_2010-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R84-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R84-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R27-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R27-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R53-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R53-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R24-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R24-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R94-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R94-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R44-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R44-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R32-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R32-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R11-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R11-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R28-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R28-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R75-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R75-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R76-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R76-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R52-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R52-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_LAMB93_R93-2009_2009-01-01/file/RPG_1-0__SHP_LAMB93_R93-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_UTM20W84GUAD_R01-2009_2009-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_UTM20W84MART_R02-2009_2009-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_UTM22RGFG95_R03-2009_2009-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2009-01-01$RPG_1-0__SHP_RGR92UTM40S_R04-2009_2009-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2009_2009-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R84-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R84-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R27-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R27-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R53-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R53-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R24-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R24-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R94-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R94-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R44-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R44-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R32-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R32-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R11-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R11-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R28-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R28-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R75-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R75-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R76-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R76-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R52-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R52-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_LAMB93_R93-2008_2008-01-01/file/RPG_1-0__SHP_LAMB93_R93-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_UTM20W84GUAD_R01-2008_2008-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_UTM20W84MART_R02-2008_2008-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_UTM22RGFG95_R03-2008_2008-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2008-01-01$RPG_1-0__SHP_RGR92UTM40S_R04-2008_2008-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2008_2008-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R84-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R84-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R27-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R27-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R53-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R53-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R24-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R24-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R94-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R94-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R44-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R44-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R32-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R32-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R11-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R11-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R28-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R28-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R75-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R75-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R76-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R76-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R52-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R52-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_LAMB93_R93-2007_2007-01-01/file/RPG_1-0__SHP_LAMB93_R93-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_UTM20W84GUAD_R01-2007_2007-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_UTM20W84MART_R02-2007_2007-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_UTM22RGFG95_R03-2007_2007-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2007_2007-01-01.7z",
"https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2007-01-01$RPG_1-0__SHP_RGR92UTM40S_R04-2007_2007-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2007_2007-01-01.7z"]

download_lst = [
    # "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R84-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R84-2012_2012-01-01.7z",
    # "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R27-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R27-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R53-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R53-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R24-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R24-2012_2012-01-01.7z ",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R94-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R94-2012_2012-01-01.7z ",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R44-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R44-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R32-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R32-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R11-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R11-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R28-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R28-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R75-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R75-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R76-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R76-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R52-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R52-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_LAMB93_R93-2012_2012-01-01/file/RPG_1-0__SHP_LAMB93_R93-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_UTM20W84GUAD_R01-2012_2012-01-01/file/RPG_1-0__SHP_UTM20W84GUAD_R01-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_UTM20W84MART_R02-2012_2012-01-01/file/RPG_1-0__SHP_UTM20W84MART_R02-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_UTM22RGFG95_R03-2012_2012-01-01/file/RPG_1-0__SHP_UTM22RGFG95_R03-2012_2012-01-01.7z",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_REGION_PACK_DIFF_2012$RPG_1-0__SHP_RGR92UTM40S_R04-2012_2012-01-01/file/RPG_1-0__SHP_RGR92UTM40S_R04-2012_2012-01-01.7z"
]
#https://data.geopf.fr/telechargement/download/RPG
download_lst = [
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG/RPG_2-0__GPKG_LAMB93_FXX_2022-01-01/RPG_2-0__GPKG_LAMB93_FXX_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R84_2022-01-01/RPG_2-0__SHP_LAMB93_R84_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R27_2022-01-01/RPG_2-0__SHP_LAMB93_R27_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R53_2022-01-01/RPG_2-0__SHP_LAMB93_R53_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R24_2022-01-01/RPG_2-0__SHP_LAMB93_R24_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R94_2022-01-01/RPG_2-0__SHP_LAMB93_R94_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R44_2022-01-01/RPG_2-0__SHP_LAMB93_R44_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R32_2022-01-01/RPG_2-0__SHP_LAMB93_R32_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R11_2022-01-01/RPG_2-0__SHP_LAMB93_R11_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R28_2022-01-01/RPG_2-0__SHP_LAMB93_R28_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R75_2022-01-01/RPG_2-0__SHP_LAMB93_R75_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R76_2022-01-01/RPG_2-0__SHP_LAMB93_R76_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R52_2022-01-01/RPG_2-0__SHP_LAMB93_R52_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_LAMB93_R93_2022-01-01/RPG_2-0__SHP_LAMB93_R93_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_UTM20W84GUAD_D971_2022-01-01/RPG_2-0__SHP_UTM20W84GUAD_D971_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_UTM20W84MART_D972_2022-01-01/RPG_2-0__SHP_UTM20W84MART_D972_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_UTM22RGFG95_D973_2022-01-01/RPG_2-0__SHP_UTM22RGFG95_D973_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_RGR92UTM40S_D974_2022-01-01/RPG_2-0__SHP_RGR92UTM40S_D974_2022-01-01.7z.001",
    "https://wxs.ign.fr/0zf5kvnyfgyss0dk5dvvq9n7/telechargement/prepackage/RPG_2-0__SHP_RGM04UTM38S_D976_2022-01-01/RPG_2-0__SHP_RGM04UTM38S_D976_2022-01-01.7z.001"
    ]


## Download
context = ssl._create_unverified_context()
output_lst = [fr"Q:\Europe-LAND\data\vector\IACS\FR\downloads\{os.path.basename(url)}" for url in download_lst]
for i, download in enumerate(download_lst):
    print(f"DL {i+1}/{len(download_lst)}, {download}")
    urllib.request.urlretrieve(download, output_lst[i])

## Unzip the files and move them
# path = r"Q:\Europe-LAND\data\vector\IACS\FR\downloads\RPG_1-0__SHP_LAMB93_R11-2007_2007-01-01.7z"
for path in output_lst:
    print(f"UZ {path}")
    ## Create output folder
    bname = os.path.basename(path)
    year = bname.split('-')[-3].split('_')[0]
    folder = rf"Q:\Europe-LAND\data\vector\IACS\FR\{year}"
    helper_functions.create_folder(folder)

    ## Unzip
    with py7zr.SevenZipFile(path, "r") as archive:
        archive.extractall(path=folder)

    ## Move the folders "up" as there are 5 subfolders
    out_pth = rf"{folder}\{bname.split('.')[0]}"
    files = glob.glob(out_pth + r'\**\*.*', recursive=True)
    for file in files:
        shutil.move(file, out_pth)

# url = "https://data.geopf.fr/annexes/ressources/wfs/agriculture.xml"
#
# for year in range(2022, 2024):
#     # Define parameters
#     params = {
#         "SERVICE": "WFS",
#         "REQUEST": "GetFeature",
#         "VERSION": "auto",
#         "TYPENAMES": f"RPG.{year}:parcelles_graphiques",
#         "outputFormat": "SHAPE-ZIP"
#     }
#
#     # Make the request
#     response = requests.get(url, params=params)
#
#     # Save response content if the request was successful
#     if response.status_code == 200:
#         with open(fr"Q:\Europe-LAND\data\vector\IACS\FR\FR\parcelles_{year}.zip", "wb") as f:
#             f.write(response.content)
#     else:
#         print(f"Error: {response.status_code}")
