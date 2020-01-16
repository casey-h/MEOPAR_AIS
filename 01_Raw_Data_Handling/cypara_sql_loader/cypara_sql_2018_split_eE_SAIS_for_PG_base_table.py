#!/usr/bin/python
# Parses out 5 message schemas from eE formatted AIS data (1/2/3,5,18,24,27) and splits into 1 file per each, 
# along with a 6th file for all other messages. Attaches a Unique ID based  on the source file and the line number, 
# and a flag indicating whether or not there was any problem in parsing the mmsi, message type, or date fields.
# The output is pipe delimited for the 5 schemas of interest, and tab delimited for the other records. For
# all message types, only the relevant fields are split from the source schema, however, only for the schemas 
# of interest are these fields prepared as to be loaded in Postgres -- for the "other" file, they are left bundled
# for loading as a varchar/text field. "\" characters are escaped to avoid issues with the Postgres \copy command.
# Values of "None", as provided by the vendor, are replaced with \N in the schemas of interest, to be interpreted 
# as nulls.

# UPDATE: had to swap \N for empty string on selected schema outputs to properly load.

from __future__ import print_function

from multiprocessing import Pool
import multiprocessing

import shutil
from glob import glob
from itertools import islice, izip_longest
from functools import partial
import sys
import os
import re
from datetime import *
import pandas as pd
import time
import gc
import math

import pyximport; pyximport.install()
from para_helpers import *

# Stderr print wrapping function
def errprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# Lock init function for handling locks within process pools.
def init(l):
    global lock
    lock = l

# Iterable by groups https://stackoverflow.com/questions/1624883/alternative-way-to-split-a-list-into-groups-of-n
def mygrouper(n, iterable):
    args = [iter(iterable)] * n
    return ([e for e in t if e != None] for t in izip_longest(*args))

# Wrapper to pass 2 arguments to function call to multiprocessing.apply.
def parse_single_file_star(indicated_filename_indicated_prefix_indicated_files):
    """Convert `f([1,2])` to `f(1,2)` call."""
    return parse_single_file(indicated_filename_indicated_prefix_indicated_files[0], indicated_filename_indicated_prefix_indicated_files[1], indicated_filename_indicated_prefix_indicated_files[2])

def parse_single_file(indicated_filename, indicated_prefix, indicated_out_filename_array):

    # Names of fields in eE AIS datafiles.
    fieldnames=['mmsi','message_id','repeat_indicator','time','millisecond','region','country','base_station','online_data','group_code','sequence_id','channel','data_length','vessel_name','call_sign','imo','ship_type','dimension_to_bow','dimension_to_stern','dimension_to_port','dimension_to_starboard','draught','destination','ais_version','navigational_status','rot','sog','accuracy','longitude','latitude','cog','heading','regional','maneuver','raim_flag','communication_flag','communication_state','utc_year','utc_month','utc_day','utc_hour','utc_minute','utc_second','fixing_device','transmission_control','eta_month','eta_day','eta_hour','eta_minute','sequence','destination_id','retransmit_flag','country_code','functional_id','data','destination_id_1','sequence_1','destination_id_2','sequence_2','destination_id_3','sequence_3','destination_id_4','sequence_4','altitude','altitude_sensor','data_terminal','mode','safety_text','non-standard_bits','name_extension','name_extension_padding','message_id_1_1','offset_1_1','message_id_1_2','offset_1_2','message_id_2_1','offset_2_1','destination_id_a','offset_a','increment_a','destination_id_b','offsetb','incrementb','data_msg_type','station_id','z_count','num_data_words','health','unit_flag','display','dsc','band','msg22','offset1','num_slots1','timeout1','increment_1','offset_2','number_slots_2','timeout_2','increment_2','offset_3','number_slots_3','timeout_3','increment_3','offset_4','number_slots_4','timeout_4','increment_4','aton_type','aton_name','off_position','aton_status','virtual_aton','channel_a','channel_b','tx_rx_mode','power','message_indicator','channel_a_bandwidth','channel_b_bandwidth','transzone_size','longitude_1','latitude_1','longitude_2','latitude_2','station_type','report_interval','quiet_time','part_number','vendor_id','mother_ship_mmsi','destination_indicator','binary_flag','gnss_status','spare','spare2','spare3','spare4']

    # Pandas types for fields in eE AIS datafiles.
    fieldtypes={'mmsi':int,'message_id':int,'repeat_indicator':'category','time':object,'millisecond':int,'region':'category','country':'category','base_station':'category','online_data':'category','group_code':'category','sequence_id':'category','channel':'category','data_length':'category','vessel_name':object,'call_sign':object,'imo':'category','ship_type':'category','dimension_to_bow':'category','dimension_to_stern':'category','dimension_to_port':'category','dimension_to_starboard':'category','draught':'category','destination':object,'ais_version':'category','navigational_status':'category','rot':float,'sog':float,'accuracy':'category','longitude':object,'latitude':object,'cog':float,'heading':float,'regional':'category','maneuver':'category','raim_flag':'category','communication_flag':'category','communication_state':object,'utc_year':'category','utc_month':'category','utc_day':'category','utc_hour':'category','utc_minute':'category','utc_second':'category','fixing_device':'category','transmission_control':'category','eta_month':'category','eta_day':'category','eta_hour':'category','eta_minute':'category','sequence':'category','destination_id':'category','retransmit_flag':'category','country_code':'category','functional_id':'category','data':object,'destination_id_1':'category','sequence_1':'category','destination_id_2':'category','sequence_2':'category','destination_id_3':'category','sequence_3':'category','destination_id_4':'category','sequence_4':'category','altitude':'category','altitude_sensor':'category','data_terminal':'category','mode':'category','safety_text':'category','non-standard_bits':'category','name_extension':'category','name_extension_padding':'category','message_id_1_1':'category','offset_1_1':'category','message_id_1_2':'category','offset_1_2':'category','message_id_2_1':'category','offset_2_1':'category','destination_id_a':'category','offset_a':'category','increment_a':'category','destination_id_b':'category','offsetb':'category','incrementb':'category','data_msg_type':'category','station_id':'category','z_count':'category','num_data_words':'category','health':'category','unit_flag':'category','display':'category','dsc':'category','band':'category','msg22':'category','offset1':'category','num_slots1':'category','timeout1':'category','increment_1':'category','offset_2':'category','number_slots_2':'category','timeout_2':'category','increment_2':'category','offset_3':'category','number_slots_3':'category','timeout_3':'category','increment_3':'category','offset_4':'category','number_slots_4':'category','timeout_4':'category','increment_4':'category','aton_type':'category','aton_name':'category','off_position':'category','aton_status':'category','virtual_aton':'category','channel_a':'category','channel_b':'category','tx_rx_mode':'category','power':'category','message_indicator':'category','channel_a_bandwidth':'category','channel_b_bandwidth':'category','transzone_size':'category','longitude_1':'category','latitude_1':'category','longitude_2':'category','latitude_2':'category','station_type':'category','report_interval':'category','quiet_time':'category','part_number':'category','vendor_id':'category','mother_ship_mmsi':'category','destination_indicator':'category','binary_flag':'category','gnss_status':'category','spare':'category','spare2':'category','spare3':'category','spare4':'category'}

    # Set of fields required for each of the message datasets.
    m123usecols=['unq_id_prefix','lineno','errorflag','mmsi','message_id','repeat_indicator','time','millisecond','region','country','base_station','online_data','group_code','sequence_id','channel','data_length','navigational_status','rot','sog','accuracy','longitude','latitude','cog','heading','maneuver','raim_flag','communication_state','utc_second','spare']
    m5usecols = ['unq_id_prefix','lineno','errorflag','mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'draught', 'destination', 'ais_version', 'fixing_device', 'transmission_control', 'eta_month', 'eta_day', 'eta_hour', 'eta_minute', 'sequence', 'data_terminal', 'mode', 'spare', 'spare2']
    m18usecols = ['unq_id_prefix','lineno','errorflag','mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'heading', 'utc_second', 'unit_flag', 'display', 'dsc', 'band', 'msg22', 'mode', 'raim_flag', 'communication_flag', 'communication_state', 'spare', 'spare2']
    m24usecols = ['unq_id_prefix','lineno','errorflag','mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'fixing_device', 'part_number', 'vendor_id', 'mother_ship_mmsi', 'spare']
    m27usecols = ['unq_id_prefix','lineno','errorflag','mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'navigational_status', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'raim_flag', 'gnss_status', 'spare']

    # Set of export columns for each message group.
    columns_m123 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'navigational_status', 'rot', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'heading', 'maneuver', 'raim_flag', 'communication_state', 'utc_second', 'spare', 'WKT']
    columns_m5 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'draught', 'destination', 'ais_version', 'fixing_device', 'transmission_control', 'eta_month', 'eta_day', 'eta_hour', 'eta_minute', 'sequence', 'data_terminal', 'mode', 'spare', 'spare2']
    columns_m18 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'heading', 'utc_second', 'unit_flag', 'display', 'dsc', 'band', 'msg22', 'mode', 'raim_flag', 'communication_flag', 'communication_state', 'spare', 'spare2', 'WKT']
    columns_m24 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'fixing_device', 'part_number', 'vendor_id', 'mother_ship_mmsi', 'spare']
    columns_m27 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'navigational_status', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'raim_flag', 'gnss_status', 'spare', 'WKT']
    columns_other = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'time', 'message_id', 'ais_msg_eecsv']
    
    print("Processing: " + indicated_filename)

    # Calculate the length of the incoming filename.
    indicated_filename_len = len(indicated_filename)
    
    # Load the entire file into a pandas dataframe:
    inner_ee_ais_datafile = pd.read_csv(indicated_filename, skiprows=1, names=fieldnames, dtype=fieldtypes,  delimiter=',', keep_default_na=False, na_values=['None','none',''], quotechar='"')
    
    # Handle malformed records:
        # Check mmsi
            # Will crash on non integer / missing mmsi.
                                
            
        #? Check string lengths

### Set error flags
            
    # Calculate a unique ID prefix value based on the input filename. Prepend the source prefix (S or T), plus 'E' to indicate exactEarth.
    # If there are dashes in the input filename, presume the format as: exactEarth_historical_data_YYYY-MM-DD.csv
    if(indicated_filename.find("-") > -1):

        unq_ID_prefix = indicated_prefix + 'E' + indicated_filename[indicated_filename_len-14:indicated_filename_len-4].replace("-","")

        # Extract the day of month suffix from the incoming date.
        day_of_month_suffix = indicated_filename[indicated_filename_len-6:indicated_filename_len-4]
    
    # If there are no dashes in the input filename, presume the format as: exactEarth_historical_data_YYYYMMDD.csv
    else:
    
        unq_ID_prefix = indicated_prefix + 'E' +  indicated_filename[indicated_filename_len-12:indicated_filename_len-4]

        # Extract the day of month suffix from the incoming date.
        day_of_month_suffix = indicated_filename[indicated_filename_len-6:indicated_filename_len-4]

    print("unq_ID_prefix: " + unq_ID_prefix )
    print("Day of month suffix: " + day_of_month_suffix )

    # Store the line number and a default errorflag in each of the records.
    inner_ee_ais_datafile['lineno'] = inner_ee_ais_datafile.index + 1 
    inner_ee_ais_datafile['unq_id_prefix'] = inner_ee_ais_datafile['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_datafile['errorflag'] = 0

# Determine if any poorly formatted dates exist, and print error messages. Implement on first instance of issue on import within PG.
    # Check date strings for length == 15 and contains "_" and remainder is numeric
#            inner_ee_ais_datafile[(inner_ee_ais_datafile['time'].str.len != 15) | (inner_ee_ais_datafile['time'].str[8] != '_'), 'errorflag'] = 1

    # Copy the original data for use in packing. 
    inner_ee_ais_datafile['orig_date'] = inner_ee_ais_datafile['time']


    # Reformat the time values into proper time strings.
    # inner_ee_ais_datafile['time'] = pd.to_datetime(inner_ee_ais_datafile['time'],format="%Y%m%d_%H%M%S").dt.strftime("%Y-%m-%d %H:%M:%S")
    #Simpler, validation-less date calculation inner_ee_ais_datafile['time'] = inner_ee_ais_datafile['time'].apply(lambda x: "{}-{}-{} {}:{}:{}".format(x[0:4],x[4:6],x[6:8],x[9:11],x[11:13],x[13:15]))
    inner_ee_ais_datafile['time'] = inner_ee_ais_datafile['time'].str[0:4] + "-" + inner_ee_ais_datafile['time'].str[4:6] + "-" + inner_ee_ais_datafile['time'].str[6:8] + " " + inner_ee_ais_datafile['time'].str[9:11] + ":" + inner_ee_ais_datafile['time'].str[11:13] + ":" + inner_ee_ais_datafile['time'].str[13:15]

    # Select a subset of message 1/2/3 data.
    inner_ee_ais_m1_2_3 = inner_ee_ais_datafile.loc[(inner_ee_ais_datafile['message_id'].isin([1,2,3])),m123usecols]

    # Select a subset of message 5 data.
    inner_ee_ais_m5 = inner_ee_ais_datafile.loc[(inner_ee_ais_datafile['message_id'] == 5),m5usecols]

    # Select a subset of message 18 data.
    inner_ee_ais_m18 = inner_ee_ais_datafile.loc[(inner_ee_ais_datafile['message_id'] == 18),m18usecols]
    
    # Select a subset of message 24 data.
    inner_ee_ais_m24 = inner_ee_ais_datafile.loc[(inner_ee_ais_datafile['message_id'] == 24),m24usecols]
    
    # Select a subset of message 27 data.
    inner_ee_ais_m27 = inner_ee_ais_datafile.loc[(inner_ee_ais_datafile['message_id'] == 27),m27usecols]

    # Select a subset of all other message data.
    inner_ee_ais_other = inner_ee_ais_datafile[(~inner_ee_ais_datafile['message_id'].isin([1,2,3,5,18,24,27]))]
    
    # Unload the parent dataframe.
    inner_ee_ais_datafile = []
    gc.collect()
    
    # Fix fractional exponents (artifact from eE processing). 
    inner_ee_ais_m1_2_3['latitude'] = inner_ee_ais_m1_2_3['latitude'].apply(lambda row: fix_exponents(row))
    inner_ee_ais_m1_2_3['longitude'] = inner_ee_ais_m1_2_3['longitude'].apply(lambda row: fix_exponents(row))
    inner_ee_ais_m18['latitude'] = inner_ee_ais_m18['latitude'].apply(lambda row: fix_exponents(row))
    inner_ee_ais_m18['longitude'] = inner_ee_ais_m18['longitude'].apply(lambda row: fix_exponents(row))
    inner_ee_ais_m27['latitude'] = inner_ee_ais_m27['latitude'].apply(lambda row: fix_exponents(row))
    inner_ee_ais_m27['longitude'] = inner_ee_ais_m27['longitude'].apply(lambda row: fix_exponents(row))

# Print message for invalid coordinates

    # Calculate rounded lat / lon for the M1/2/3 data 
    inner_ee_ais_m1_2_3['round_lat'] = inner_ee_ais_m1_2_3['latitude'].apply(lambda row: round(convert_float(row)), 2)
    inner_ee_ais_m1_2_3['round_lon'] = inner_ee_ais_m1_2_3['longitude'].apply(lambda row: round(convert_float(row)), 2)

    inner_ee_ais_m18['float_lat'] = inner_ee_ais_m18['latitude'].apply(lambda row: convert_float(row))
    inner_ee_ais_m18['float_lon'] = inner_ee_ais_m18['longitude'].apply(lambda row: convert_float(row))
    inner_ee_ais_m27['float_lat'] = inner_ee_ais_m27['latitude'].apply(lambda row: convert_float(row))
    inner_ee_ais_m27['float_lon'] = inner_ee_ais_m27['longitude'].apply(lambda row: convert_float(row))

    # Add WKT fields for m1/2/3, m18 and m27 subsets where coordinates are valid.
    inner_ee_ais_m1_2_3['WKT'] = "SRID=4326;POINT(" + inner_ee_ais_m1_2_3['longitude'] + " " + inner_ee_ais_m1_2_3['latitude'] + ")"
    inner_ee_ais_m18['WKT'] = "SRID=4326;POINT(" + inner_ee_ais_m18['longitude'] + " " + inner_ee_ais_m18['latitude'] + ")"
    inner_ee_ais_m27['WKT'] = "SRID=4326;POINT(" + inner_ee_ais_m27['longitude'] + " " + inner_ee_ais_m27['latitude'] + ")"
    
                
    # Filter out invalid WKT values and set to empty strings.
    inner_ee_ais_m1_2_3.loc[(inner_ee_ais_m1_2_3.round_lon < -180) | (inner_ee_ais_m1_2_3.round_lon > 180) | (inner_ee_ais_m1_2_3.round_lat < -90) | (inner_ee_ais_m1_2_3.round_lat > 90), 'WKT']= ''
    inner_ee_ais_m18.loc[(inner_ee_ais_m18.float_lon < -180) | (inner_ee_ais_m18.float_lon > 180) | (inner_ee_ais_m18.float_lat < -90) | (inner_ee_ais_m18.float_lat > 90), 'WKT']= ''
    inner_ee_ais_m27.loc[(inner_ee_ais_m27.float_lon < -180) | (inner_ee_ais_m27.float_lon > 180) | (inner_ee_ais_m27.float_lat < -90) | (inner_ee_ais_m27.float_lat > 90), 'WKT']= ''
    
    # Add unq_id _prefix fields to all datasets.
    inner_ee_ais_m1_2_3['unq_id_prefix'] = inner_ee_ais_m1_2_3['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_m5['unq_id_prefix'] = inner_ee_ais_m5['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_m18['unq_id_prefix'] = inner_ee_ais_m18['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_m24['unq_id_prefix'] = inner_ee_ais_m24['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_m27['unq_id_prefix'] = inner_ee_ais_m27['lineno'].apply(lambda row: unq_ID_prefix)
    inner_ee_ais_other['unq_id_prefix'] = inner_ee_ais_other['lineno'].apply(lambda row: unq_ID_prefix)

    # Sort M1/2/3 data on rounded lon, lat, and line number / received order.
    inner_ee_ais_m1_2_3.sort_values(by=['round_lon','round_lat','lineno'], inplace=True)

    # Create a packed row for records in the "other" record set.
    inner_ee_ais_other['ais_msg_eecsv'] = inner_ee_ais_other.apply(lambda row: create_packed_other_rows(row['message_id'], row), axis=1)
    
    # Export to separate files by day and deallocate.
    inner_ee_ais_m1_2_3.to_csv(indicated_out_filename_array[0] + '(' + day_of_month_suffix + ').txt', '|', '', mode="a", columns=columns_m123, header=False, index=False)
    inner_ee_ais_m1_2_3 = []
    gc.collect()

    inner_ee_ais_m5.to_csv(indicated_out_filename_array[1] + '(' + day_of_month_suffix + ').txt', '|', '', mode="a", columns=columns_m5, header=False, index=False, quotechar="~")
    inner_ee_ais_m5 = []
    gc.collect()

    inner_ee_ais_m18.to_csv(indicated_out_filename_array[2] + '(' + day_of_month_suffix + ').txt', '|', '', mode="a", columns=columns_m18, header=False, index=False)
    inner_ee_ais_m18 = []
    gc.collect()
    
    inner_ee_ais_m24.to_csv(indicated_out_filename_array[3] + '(' + day_of_month_suffix + ').txt', '|', '', mode="a", columns=columns_m24, header=False, index=False, quotechar="~")
    inner_ee_ais_m24 = []
    gc.collect()
    
    inner_ee_ais_m27.to_csv(indicated_out_filename_array[4] + '(' + day_of_month_suffix + ').txt', '|', '', mode="a", columns=columns_m27, header=False, index=False)
    inner_ee_ais_m27 = []
    gc.collect()
    
    inner_ee_ais_other.to_csv(indicated_out_filename_array[5] + '(' + day_of_month_suffix + ').txt', '\t', '\N', mode="a", columns=columns_other, header=False, index=False, quotechar="~")
    inner_ee_ais_other = []
    gc.collect()
    
def main():
       
    # Usage string for the script.
    USAGE_STRING = """Usage: cypara_sql_2018_split_eE_SAIS_for_PG_base_table.py numprocesses outputfilenameprefix inputfilename1 [inputfilename2 ...]
    Validates 7 basic message types 1,2,3,5,18,24,27 from eE formatted 
    AIS data, generates a unique line ID. Outputs separate files for each of 
    the most populous / used types (1/2/3,5,18,24,27), plus one file with all 
    other data and validated fields extracted. Inserts the 3 generated fields 
    Message type + date, line number and flag as result of validation along 
    with the original line, all in a tab delimited output file. Presumes 
    files in the format 'exactEarth_historical_data_YYYY-MM-DD.csv' or 
    'exactEarth_historical_data_YYYYMMDD.csv'. Uses up to numprocesses processes
    to perform calculation."""

    # Establish the start time for processing
    t_filestart = time.time()

    # Set of export columns for each message group.
    columns_m123 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'navigational_status', 'rot', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'heading', 'maneuver', 'raim_flag', 'communication_state', 'utc_second', 'spare', 'WKT']
    columns_m5 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'draught', 'destination', 'ais_version', 'fixing_device', 'transmission_control', 'eta_month', 'eta_day', 'eta_hour', 'eta_minute', 'sequence', 'data_terminal', 'mode', 'spare', 'spare2']
    columns_m18 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'heading', 'utc_second', 'unit_flag', 'display', 'dsc', 'band', 'msg22', 'mode', 'raim_flag', 'communication_flag', 'communication_state', 'spare', 'spare2', 'WKT']
    columns_m24 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'vessel_name', 'call_sign', 'imo', 'ship_type', 'dimension_to_bow', 'dimension_to_stern', 'dimension_to_port', 'dimension_to_starboard', 'fixing_device', 'part_number', 'vendor_id', 'mother_ship_mmsi', 'spare']
    columns_m27 = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'message_id', 'repeat_indicator', 'time', 'millisecond', 'region', 'country', 'base_station', 'online_data', 'group_code', 'sequence_id', 'channel', 'data_length', 'navigational_status', 'sog', 'accuracy', 'longitude', 'latitude', 'cog', 'raim_flag', 'gnss_status', 'spare', 'WKT']
    columns_other = ['unq_id_prefix', 'lineno', 'errorflag', 'mmsi', 'time', 'message_id', 'ais_msg_eecsv']

    # If at least three arguments are not provided, display an usage message.
    if (len(sys.argv) < 4):
        print(USAGE_STRING)
        quit()

    # Set the unique row id prefix to indicate Satellite (sat)
    source_prefix = "S"
        
    # Retrieve the number of processes with which to calculate.
    try:
        num_processes = int(sys.argv[1])
    except:
        print("Error {}, is not a valid number of processes for calculation.",sys.argv[1])
        print(USAGE_STRING)
        quit()   

    # retrieve the output filename prefix.
    out_filename_prefix = sys.argv[2]

    # Build an output filename for the .sql file which will contain 
    # a sql commands to create the target tables and populate them. 
    # Abort if the file already exists.
    out_sql_load_filename = out_filename_prefix + "_load.sql"
    if os.path.exists(out_sql_load_filename):
            print("Error, output file exists: (" + out_sql_load_filename +  ") aborting.")
            quit()

    # Build the output filenames to span the various types of message output.
#    out_filename_array = [out_filename_prefix + "_1_2_3.txt", out_filename_prefix + "_5.txt", out_filename_prefix + "_18.txt", out_filename_prefix + "_24.txt", out_filename_prefix + "_27.txt", out_filename_prefix + "_other.txt"]
    out_filename_array = [out_filename_prefix + "_1_2_3", out_filename_prefix + "_5", out_filename_prefix + "_18", out_filename_prefix + "_24", out_filename_prefix + "_27", out_filename_prefix + "_other"]
    
    # Check the output files for existence before running.
    for out_filename in out_filename_array:
        if os.path.exists(out_filename):
            print("Error, output file exists: (" + out_filename +  ") aborting.")
            quit()

    # Establish a lock to be used by child processes to arbitrate file writing.
    l = multiprocessing.Lock()

    # Establish a pool of processes to compute the requested ETL.
    try:

# Added locking to allow data writes        pool = Pool(processes=num_processes)
        pool = multiprocessing.Pool(initializer=init, initargs=(l,), processes=num_processes)
    except:
        print("Error, unable to build pool of {} processes for calculation.",num_processes)
        print(USAGE_STRING)
        quit()

    # Create a an SQL file to load the indicated files once they're finished.
    write_sql_load_script(out_sql_load_filename, out_filename_prefix, out_filename_array)

    # Process each input file reference passed as input.
    for in_filename in mygrouper(num_processes,sys.argv[3:]):
                    

        # Build a list of filenames w/ source prefix to parse.
        parse_list = []
        for nameval in in_filename:
            parse_list.append([nameval, source_prefix, out_filename_array])
                    
        # Map the parser calls onto the pool of available workers.
        out_dfs = pool.map(parse_single_file_star, parse_list)
                
# Only if needed by PG: Replace \\ w \\\\ .replace("\\","\\\\"), reenable quoting.
        
    # Aggregate multiple output files.
    for out_filename_prefix in out_filename_array:
        # Obtain target files in name-sorted order.
        files_to_aggregate = sorted(glob(out_filename_prefix + "(*).txt"))
        with open(out_filename_prefix + '.txt','wb') as wfd:

            for f in files_to_aggregate:
                errprint('Aggregating: {}'.format(f))
                with open(f,'rb') as fd:
                    shutil.copyfileobj(fd, wfd, -1)
                    
                # Remove the aggregated file.
                os.remove(f)
        
    # Print out the time to write outputs
    t_fileelapsed = time.time() - t_filestart
    errprint("Output records written, {} seconds.".format(t_fileelapsed))

# If we're invoked directly (which we generally expect), run.
if __name__ == "__main__":
    main()
