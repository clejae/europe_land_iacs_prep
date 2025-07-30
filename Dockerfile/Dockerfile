###########################################################################
# :AUTHOR: Katarzyna Ewa Lewinska  [lewinska@hu-berlin.de]
# :DATE: 24 July 2025
#
# :Description: A Docker image for a container to run 
#               https://github.com/clejae/europe_land_iacs_prep
#
# :Updates:    
#
# :2Do:
#
# :Disclaimer:  The author of this code accepts no responsibility for errors or omissions in this work
#               and shall not be liable for any damage caused by these.
###########################################################################

# Use a slim Python base image
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    curl \
    git \
    wget \
    libgdal-dev \
    gdal-bin \
    libffi-dev \
    libpq-dev \
    libxml2-dev \
    libproj-dev \
    libgeos-dev \
    libspatialindex-dev \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_VERSION=3.6.0
ENV PROJ_VERSION=9.1.0

RUN pip install --upgrade pip

RUN pip install \
    gdal==${GDAL_VERSION} \
    pandas \
    chardet \
    geopandas \
    openpyxl \
    pyarrow \
    seaborn \
    deep-translator \
    jaro-winkler

CMD ["python"]
