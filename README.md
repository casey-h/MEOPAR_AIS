# MEOPAR_AIS

A collection of Python scripts for processing AIS data in support of the MEOPAR (meopar.ca) - exactEarth (exactearth.ca) - Dalhousie (bigdata.cs.dal.ca) Satellite AIS data partnership / initiative. Code is a work in progress and should be considered functional, yet in flux.

01_Raw_Data_Handling - Scripts for processing raw (NM4) and flat (csv) AIS datafiles

0_DMAS_TAIS_NM4_parsing.py - Parsing script for ONC / DMAS NM4 flat files into csv.
0_gpsd_eE_ais_NM4_parsing.py - Parsing script for translating exactEarth formatted NM4 flat files into csv.
Renamed from 0a_gpsd_eE_ais_NM4_parsing.py

0_taggart_TAIS_pre_parser.py - Parsing script for translating Dr. Chris Taggart T-AIS network formatted NM4 flat files into csv.
Renamed from 0b_taggart_TAIS_pre_parser.py

cypara_sql_loader/cypara_sql_2018_split_eE_SAIS_for_PG_base_table.py - Parallelized Script to parse comma delimited Postgres DB data files of exactEarth AIS data into separate files on the basis of message type. Prepends headers denoting fields present on a per-message type basis. Current supports data files of AIS type groups: 1+2+3, 4+11, 5, 18+19. Rewritten with Cython, uses revised table schema (circa 2018-01).
Replaces: Parse_eE_AIS_PG_Exports_to_csv.py 

split_ONC_AIS_for_PG_base_table_w_parsing.py - Script to parse csv AIS data as provided by Ocean Networks Canada DMAS data service into compact form to match local Postgres Schema.
split_tT_AIS_for_PG_base_table_w_parsing.py - Script to parse csv AIS data as provided by Dr. Chris Taggart Terrestrial AIS network into compact form to match local Postgres Schema.

02_Segment_Development - Scripts for building geospatial segment and trajectory representations from AIS position data.

0_split_eE_AIS_pre_tracks.py - Soon to be obsoleted. see 1_generate_tracks_from_AIS_DB_vectorized.py
0_split_NM4_Sourced_AIS_pre_tracks.py - Soon to be obsolete. see 1_generate_tracks_from_AIS_DB_vectorized.py
1_generate_tracks_from_TAIS_ONC.py - Soon to be obsoleted. Script to generate tracks from ONC formatted AIS data, will be incorporated into 1_generate_tracks_from_AIS_DB_vectorized.py.


1_generate_tracks_from_AIS_DB_vectorized.py - New aggregate script, performs functionality of old 1_, 2_ and 3_ scripts together. Starting with a file of exported SAIS data from the Postgres database instance, splits it on type, then vessel, generates either segments or tracklines and finally creates a GIS representation of same. Can currently load data from Postgres DB or csv; eventually will also accept NM4 data.
1_generate_tracks_from_AIS_DB_vectorized.pyc

03_Grid_Calculations

0_create_grids_gdal.py - A script to generate a regular grid, suitable for use in aggregation of tracks.

1_tracks_into_grids_gdal.py - A script to take a layer of segment or grid data, as generated by 1_generate_tracks_from_AIS_DB_vectorized.py along with a regular polygon grid, and calculate the aggregated intersection of the polylines into the grids. Output is generated in shapefile format.
1_tracks_into_grids_gdal_to_text.py - A script to take a layer of segment or grid data, as generated by 1_generate_tracks_from_AIS_DB_vectorized.py along with a regular polygon grid, and calculate the aggregated intersection of the polylines into the grids. Output is generated in text format.

1_seg_interp_into_grids.py - (Prototype) A script to take a layer of segment or grid data, as generated by 1_generate_tracks_from_AIS_DB_vectorized.py along with a regular polygon grid, and calculate the aggregated intersection of the polylines into the grids. Adds interpolation of several  Output is generated in shapefile format.
1_seg_interp_into_grids_w_date.py - (Prototype) A script to take a layer of segment or grid data, as generated by 1_generate_tracks_from_AIS_DB_vectorized.py along with a regular polygon grid, and calculate the aggregated intersection of the polylines into the grids. Output is generated in text format.

