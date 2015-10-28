# MEOPAR_AIS


# Utility scripts:

split_eE_AIS_for_PG_base_table_w_parsing.py - Script to parse csv AIS data as provided by exactEarth into compact form to match local Postgres Schema.

Parse_eE_AIS_PG_Exports_to_csv.py - Script to parse comma delimited Postgres DB data files of exactEarth AIS data into separate files on the basis of message type. Prepends headers denoting fields present on a per-message type basis. Current supports data files of AIS type groups: 1+2+3, 4+11, 5, 18+19. 


# Trajectory generation scripts:

0_gpsd_ais_NM4_parsing.py - Script to parse static files of NM4 data, based on the python streamed-AIS reader from GPSD. 

1a_split_eE_AIS_pre_tracks.py - Script to split tab-delimited Postgres DB data files of exactEarth position-referenced AIS data into basic movement data files on message type group, with aim to trajectory generation. Generates files of Messages 1+2+3; 18+19 and "Other".

1b_split_NM4_Sourced_AIS_pre_tracks.py

2_divide_tracks.py

3e_build_tracklines_and_points_gdal.py
    - Requires GDAL to be installed and available to Python

# Project specific scripts:

----- NEMES

2x_divide_short_tracks_v_NEMES.py

3a_build_tracklines_and_points_NEMES.py
    - Requires QGIS / pyqgis to be installed and available to Python

3b_build_interp_tracklines_and_points_NEMES.py
    - Requires QGIS / pyqgis to be installed and available to Python

3c_build_tracklines_and_points_pyshp_NEMES.py
    - Requires pyshp (https://github.com/GeospatialPython/pyshp)
    - Slow

3d_build_tracklines_and_points_fiona_NEMES.py
    - Requires fiona + GDAL to be installed and available to Python

----- WormLab

Split_eE_AIS_wormdata_mvmt_on_MMSI.py

Split_eE_AIS_wormdata_type5_on_MMSI.py

----- BigData

Track_labeling_build_points_gdal_BigData.py

Track_labeling_build_tracklines_gdal_BigData.py