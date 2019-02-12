#!/usr/bin/python
# Helper files for para_sql_2008_split_eE_SAIS_for_PG_base_table.py


import re
from datetime import *
import pandas as pd
import time
import math

# Stackoverflow sourced short f'n for testing whether / not string values are numeric, modified from 
# source to account for multiple number types. 
# Type-value ranges for PG: http://www.postgresql.org/docs/9.1/static/datatype-numeric.html
cpdef int is_float(str s):
    try:
        float(s)
        return 0
    except ValueError:
        return -1
        
cpdef int is_integer(str s):
    try:
        inttest = int(s)
        #Ignores possibility of -2147483648, considered acceptable compromise for performance, value 
        #shouldn't exist in AIS data.
        if abs(inttest) < 2147483648:
            return 0
        else:
            return -1
    except ValueError:
        return -1

cpdef float convert_float(str s):
    
    if (is_float(s) != 0): 
    
        if (s.endswith('E+1.0')):
            print "Translating: " + s + " to: " + s[:-2]
            return float(s[:-2])

        elif (s.endswith('E+2.0')):
            print "Translating: " + s + " to: " + s[:-2]
            return float(s[:-2])

        else: 
            print "Coordinate parse error, aborting."
            quit()

    else:
        return float(s)

cpdef str fix_exponents(str s):
    
    if (is_float(s) != 0): 
    
        if (s.endswith('E+1.0')):
            print "Translating: " + s + " to: " + s[:-2]
            return s[:-2]

        elif (s.endswith('E+2.0')):
            print "Translating: " + s + " to: " + s[:-2]
            return s[:-2]

        else: 
            print "Coordinate parse error, aborting."
            quit()

    else:
        return s
   
#def validate_date(str in_raw_date_string, parse_error_flag):
#
#    try:
#        parsed_date_val = datetime.strptime(in_raw_date_string, "%Y%m%d_%H%M%S")
#
#        # If the date is ok, build a Postgres-acceptable timestamp from the parsed value.
#        #e.g 20141001_000005 -> 2014-10-01 00:00:05
#        parsed_date_string = parsed_date_val.strftime("%Y-%m-%d %H:%M:%S")
#    except:
#        
#        parse_error_flag = True
#        parsed_date_string = r"\N"
#    
#    return (parsed_date_string, parse_error_flag)
              
# Check to see that the filename prefix given is the correct length and has a valid month integer.
def validate_filename_prefix(str in_filename_prefix):
    
    if (len(in_filename_prefix) <> 12) or is_integer(in_filename_prefix[6:10]) != 0 or not in_filename_prefix[11:13]:
        return False
    else:
        if (int(in_filename_prefix[10:12]) <= 12):
            return True
        else:
            return False

# Calculate EWKT Points from a coordinate pair, filtering out non-numeric coordinates and those
# with values abs(lon) > 180 or abs(lat) > 90.
#def create_point_EWKT(float longitude, float latitude):
#    if math.isnan(longitude) or math.isnan(latitude):
#        return ""
#    elif (abs(longitude) > 180 or abs(latitude) > 90) :
#       return ""
#    else:
#       return "EPSG=4326;POINT(" + str(longitude) + " " + str(latitude) + ")" 
    
# write_pgloader_load_script - Writes out a script, under the filename: out_sql_load_filename, which can be 
# run by pgloader to create the template tables required to hold the parsed data, and then load and index them. 
# The out_filename_prefix is presumed to be a string of the form ("ais_s_yyyymm"), and from which the month of 
# the data will be extracted -- malformed prefixes will cause an error. The names of the output files are 
# expected in out_filename_array, corresponding to message 1/2/3, 5, 18, 24, 27 and  all others (6 files, fixed order). 
def write_sql_load_script(str out_sql_load_filename, str out_filename_prefix, out_filename_array):

    # Attempt to parse the month and year from the incoming filename prefix and determine the subsequent month.
    if  validate_filename_prefix(out_filename_prefix):
        
        prefyear = int(out_filename_prefix[6:10])
        prefmonth = int(out_filename_prefix[10:12])
        
        if (prefmonth < 12):
            next_prefyear = prefyear
            next_prefmonth = prefmonth + 1
        else:
            next_prefyear = prefyear + 1
            next_prefmonth = 1            
        
        try:
            out_sql_file = open(out_sql_load_filename, 'w')

            # MSG 123
            out_sql_file.write(
            "drop table if exists " + out_filename_prefix + "_msg_1_2_3; "
            "create table " + out_filename_prefix + "_msg_1_2_3 partition of ee_ais_master_1_2_3 for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY " + out_filename_prefix + "_msg_1_2_3(unq_id_prefix,lineno,errorflag,mmsi,message_id,repeat_indicator,time,millisecond,region,country,base_station,online_data,group_code,sequence_id,channel,data_length,navigational_status,rot,sog,accuracy,longitude,latitude,cog,heading,maneuver,raim_flag,communication_state,utc_second,spare,ais_geom) from '" + out_filename_array[0] + ".txt' CSV delimiter '|'\n" +
            "create index on " + out_filename_prefix + "_msg_1_2_3(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_1_2_3 using BRIN(time) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_1_2_3 using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_1_2_3 using BRIN(ais_geom) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_1_2_3 using BRIN(ais_geom, time) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_1_2_3;\n" +
            "\n")
            
            # MSG 5
            out_sql_file.write(
            "drop table if exists " + out_filename_prefix + "_msg_5;\n" +
            "create table " + out_filename_prefix + "_msg_5 partition of ee_ais_master_5 for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY " + out_filename_prefix + "_msg_5 from '" + out_filename_array[1] + ".txt' CSV delimiter '|' QUOTE '~'\n" +
            "create index on " + out_filename_prefix + "_msg_5(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_5 using BRIN(time) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_5 using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_5;\n" +
            "\n")
            
            # MSG 18
            out_sql_file.write(
            "drop table if exists " + out_filename_prefix + "_msg_18;\n" +
            "create table " + out_filename_prefix + "_msg_18 partition of ee_ais_master_18 for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY " + out_filename_prefix + "_msg_18(unq_id_prefix,lineno,errorflag,mmsi,message_id,repeat_indicator,time,millisecond,region,country,base_station,online_data,group_code,sequence_id,channel,data_length,sog,accuracy,longitude,latitude,cog,heading,utc_second,unit_flag,display,dsc,band,msg22,mode,raim_flag,communication_flag,communication_state,spare,spare2,ais_geom) FROM '" + out_filename_array[2] + ".txt' CSV delimiter '|'\n" + 
            "create index on " + out_filename_prefix + "_msg_18(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_18 using BRIN(time) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_18 using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_18 using BRIN(ais_geom) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_18 using BRIN(ais_geom, time) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_18;\n" +
            "\n")

            # MSG 24
            out_sql_file.write(
            "drop table if exists " + out_filename_prefix + "_msg_24;\n" +
            "create table " + out_filename_prefix + "_msg_24 partition of ee_ais_master_24 for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY " + out_filename_prefix + "_msg_24 FROM '" + out_filename_array[3] + ".txt' CSV delimiter '|' QUOTE '~'\n" +
            "create index on " + out_filename_prefix + "_msg_24(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_24 using BRIN(time) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_24 using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_24;\n" +
            "\n")

            # MSG 27
            out_sql_file.write(
            "drop table if exists " + out_filename_prefix + "_msg_27;\n" +
            "create table " + out_filename_prefix + "_msg_27 partition of ee_ais_master_27 for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY " + out_filename_prefix + "_msg_27(unq_ID_prefix,lineno,errorflag,MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Navigational_status,SOG,Accuracy,Longitude,Latitude,COG,RAIM_flag,GNSS_status,spare,ais_geom) FROM '" + out_filename_array[4] + ".txt' CSV delimiter '|'\n"
            "create index on " + out_filename_prefix + "_msg_27(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_27 using BRIN(time) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_27 using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_27 using BRIN(ais_geom) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_27 using BRIN(ais_geom, time) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_27;\n" +
            "\n")

            # Other MSGS
            out_sql_file.write(            
            "drop table if exists " + out_filename_prefix + "_msg_other;\n" +
            "create table " + out_filename_prefix + "_msg_other partition of ee_ais_master_other for values from ('{:04d}-{:02d}-01') to ('{:04d}-{:02d}-01');\n".format(prefyear, prefmonth, next_prefyear, next_prefmonth) + 
            "\COPY "  + out_filename_prefix + "_msg_other FROM '" + out_filename_array[5] + ".txt' CSV DELIMITER E'\\t' ESCAPE '~' QUOTE '`'\n"
            "create index on " + out_filename_prefix + "_msg_other(mmsi) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_other using BRIN(datetime) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_other using BRIN(unq_id_prefix,lineno) TABLESPACE index_tablespace;\n" +
            "create index on " + out_filename_prefix + "_msg_other(message_id) TABLESPACE index_tablespace;\n" +
            "vacuum analyze " + out_filename_prefix + "_msg_other;\n" +
            "\n")

            out_sql_file.close()

        except IOError:
            print "Error opening pgloader output file: " + out_sql_load_filename + "\n"
            quit()
    else:
        print "Error parsing out_filename prefix, expected form: ais_s_yyyymm"
        quit()


# create_packed_other_rows - A helper function to build a pipe delimited string, 
# containing the fields corresponding to the message_id_value, from elements of 
# the row_values input dataframe row.
cpdef str create_packed_other_rows(int message_id_value, row_values):
    cdef str packed_data

    #4_11
    if(message_id_value in (4, 11)):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['accuracy']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['raim_flag']), str(row_values['communication_state']), str(row_values['utc_year']), str(row_values['utc_month']), str(row_values['utc_day']), str(row_values['utc_hour']), str(row_values['utc_minute']), str(row_values['utc_second']), str(row_values['fixing_device']), str(row_values['transmission_control']), str(row_values['spare'])])
        
    #6 
    elif(message_id_value == 6):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['sequence']), str(row_values['destination_id']), str(row_values['retransmit_flag']), str(row_values['country_code']), str(row_values['functional_id']), str(row_values['data']), str(row_values['spare'])])

    #7_13
    elif(message_id_value in (7, 13)):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['destination_id_1']), str(row_values['sequence_1']), str(row_values['destination_id_2']), str(row_values['sequence_2']), str(row_values['destination_id_3']), str(row_values['sequence_3']), str(row_values['destination_id_4']), str(row_values['sequence_4']), str(row_values['spare'])])
        
    #8 
    elif(message_id_value == 8):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['country_code']), str(row_values['functional_id']), str(row_values['data']), str(row_values['spare'])])
        
    #9 
    elif(message_id_value == 9):
        
        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['sog']), str(row_values['accuracy']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['cog']), str(row_values['raim_flag']), str(row_values['communication_flag']), str(row_values['communication_state']), str(row_values['utc_second']), str(row_values['altitude']), str(row_values['altitude_sensor']), str(row_values['data_terminal']), str(row_values['mode']), str(row_values['spare'])])

    #10 
    elif(message_id_value == 10):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['regional']), str(row_values['destination_id']), str(row_values['spare'])])

    #12 
    elif(message_id_value == 12):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['sequence']), str(row_values['destination_id']), str(row_values['retransmit_flag']), str(row_values['safety_text']), str(row_values['non-standard_bits']), str(row_values['spare'])])
        
    #14 
    elif(message_id_value == 14):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['safety_text']), str(row_values['non-standard_bits']), str(row_values['name_extension']), str(row_values['spare']), str(row_values['spare2'])])

    #15
    elif(message_id_value == 15):
        
        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['destination_id_1']), str(row_values['destination_id_2']), str(row_values['message_id_1_1']), str(row_values['offset_1_1']), str(row_values['message_id_1_2']), str(row_values['offset_1_2']), str(row_values['message_id_2_1']), str(row_values['offset_2_1']), str(row_values['spare'])])
        
    #16
    elif(message_id_value == 16):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['destination_id_a']), str(row_values['offset_a']), str(row_values['increment_a']), str(row_values['destination_id_b']), str(row_values['offsetb']), str(row_values['incrementb']), str(row_values['spare'])])

    #17 
    elif(message_id_value == 17):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['regional']), str(row_values['data']), str(row_values['data_msg_type']), str(row_values['station_id']), str(row_values['z_count']), str(row_values['num_data_words']), str(row_values['health']), str(row_values['spare'])])

    #19
    elif(message_id_value == 19):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['vessel_name']), str(row_values['ship_type']), str(row_values['dimension_to_bow']), str(row_values['dimension_to_stern']), str(row_values['dimension_to_port']), str(row_values['dimension_to_starboard']), str(row_values['sog']), str(row_values['accuracy']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['cog']), str(row_values['heading']), str(row_values['regional']), str(row_values['raim_flag']), str(row_values['communication_flag']), str(row_values['communication_state']), str(row_values['utc_second']), str(row_values['fixing_device']), str(row_values['data_terminal']), str(row_values['mode']), str(row_values['unit_flag']), str(row_values['display']), str(row_values['dsc']), str(row_values['band']), str(row_values['msg22']), str(row_values['spare']), str(row_values['spare2'])])
        
    #20
    elif(message_id_value == 20):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['offset1']), str(row_values['num_slots1']), str(row_values['timeout1']), str(row_values['increment_1']), str(row_values['offset_2']), str(row_values['number_slots_2']), str(row_values['timeout_2']), str(row_values['increment_2']), str(row_values['offset_3']), str(row_values['number_slots_3']), str(row_values['timeout_3']), str(row_values['increment_3']), str(row_values['offset_4']), str(row_values['number_slots_4']), str(row_values['timeout_4']), str(row_values['increment_4']), str(row_values['spare'])])

    #21 
    elif(message_id_value == 21):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['dimension_to_bow']), str(row_values['dimension_to_stern']), str(row_values['dimension_to_port']), str(row_values['dimension_to_starboard']), str(row_values['accuracy']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['raim_flag']), str(row_values['utc_second']), str(row_values['fixing_device']), str(row_values['mode']), str(row_values['name_extension']), str(row_values['name_extension_padding']), str(row_values['aton_type']), str(row_values['aton_name']), str(row_values['off_position']), str(row_values['aton_status']), str(row_values['virtual_aton']), str(row_values['channel_a']), str(row_values['spare']), str(row_values['spare2'])])
        
    #22
    elif(message_id_value == 22):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['channel_a']), str(row_values['channel_b']), str(row_values['tx_rx_mode']), str(row_values['power']), str(row_values['message_indicator']), str(row_values['channel_a_bandwidth']), str(row_values['channel_b_bandwidth']), str(row_values['transzone_size']), str(row_values['longitude_1']), str(row_values['latitude_1']), str(row_values['longitude_2']), str(row_values['latitude_2']), str(row_values['spare'])])
        
    #23
    elif(message_id_value == 23):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['ship_type']), str(row_values['tx_rx_mode']), str(row_values['longitude_1']), str(row_values['latitude_1']), str(row_values['longitude_2']), str(row_values['latitude_2']), str(row_values['station_type']), str(row_values['report_interval']), str(row_values['quiet_time']), str(row_values['spare'])])

    #25 
    elif(message_id_value == 25):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['destination_id']), str(row_values['country_code']), str(row_values['functional_id']), str(row_values['data']), str(row_values['destination_indicator']), str(row_values['binary_flag'])])
        
    #26 
    elif(message_id_value == 26):

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['communication_flag']), str(row_values['communication_state']), str(row_values['destination_id']), str(row_values['country_code']), str(row_values['functional_id']), str(row_values['data']), str(row_values['destination_indicator']), str(row_values['binary_flag'])])
        
    else:

        print("Message type parse error.(" + row_values['unq_id_prefix'] + "_" + str(row_values['lineno']) + ": " + row_values['message_id'] + " parsed as:" + str(message_id_value) + ")\n")

        packed_data = "|".join([str(row_values['mmsi']), str(row_values['message_id']), str(row_values['repeat_indicator']), str(row_values['orig_date']), str(row_values['millisecond']), str(row_values['region']), str(row_values['country']), str(row_values['base_station']), str(row_values['online_data']), str(row_values['group_code']), str(row_values['sequence_id']), str(row_values['channel']), str(row_values['data_length']), str(row_values['vessel_name']), str(row_values['call_sign']), str(row_values['imo']), str(row_values['ship_type']), str(row_values['dimension_to_bow']), str(row_values['dimension_to_stern']), str(row_values['dimension_to_port']), str(row_values['dimension_to_starboard']), str(row_values['draught']), str(row_values['destination']), str(row_values['ais_version']), str(row_values['navigational_status']), str(row_values['rot']), str(row_values['sog']), str(row_values['accuracy']), str(row_values['longitude']), str(row_values['latitude']), str(row_values['cog']), str(row_values['heading']), str(row_values['regional']), str(row_values['maneuver']), str(row_values['raim_flag']), str(row_values['communication_flag']), str(row_values['communication_state']), str(row_values['utc_year']), str(row_values['utc_month']), str(row_values['utc_day']), str(row_values['utc_hour']), str(row_values['utc_minute']), str(row_values['utc_second']), str(row_values['fixing_device']), str(row_values['transmission_control']), str(row_values['eta_month']), str(row_values['eta_day']), str(row_values['eta_hour']), str(row_values['eta_minute']), str(row_values['sequence']), str(row_values['destination_id']), str(row_values['retransmit_flag']), str(row_values['country_code']), str(row_values['functional_id']), str(row_values['data']), str(row_values['destination_id_1']), str(row_values['sequence_1']), str(row_values['destination_id_2']), str(row_values['sequence_2']), str(row_values['destination_id_3']), str(row_values['sequence_3']), str(row_values['destination_id_4']), str(row_values['sequence_4']), str(row_values['altitude']), str(row_values['altitude_sensor']), str(row_values['data_terminal']), str(row_values['mode']), str(row_values['safety_text']), str(row_values['non-standard_bits']), str(row_values['name_extension']), str(row_values['name_extension_padding']), str(row_values['message_id_1_1']), str(row_values['offset_1_1']), str(row_values['message_id_1_2']), str(row_values['offset_1_2']), str(row_values['message_id_2_1']), str(row_values['offset_2_1']), str(row_values['destination_id_a']), str(row_values['offset_a']), str(row_values['increment_a']), str(row_values['destination_id_b']), str(row_values['offsetb']), str(row_values['incrementb']), str(row_values['data_msg_type']), str(row_values['station_id']), str(row_values['z_count']), str(row_values['num_data_words']), str(row_values['health']), str(row_values['unit_flag']), str(row_values['display']), str(row_values['dsc']), str(row_values['band']), str(row_values['msg22']), str(row_values['offset1']), str(row_values['num_slots1']), str(row_values['timeout1']), str(row_values['increment_1']), str(row_values['offset_2']), str(row_values['number_slots_2']), str(row_values['timeout_2']), str(row_values['increment_2']), str(row_values['offset_3']), str(row_values['number_slots_3']), str(row_values['timeout_3']), str(row_values['increment_3']), str(row_values['offset_4']), str(row_values['number_slots_4']), str(row_values['timeout_4']), str(row_values['increment_4']), str(row_values['aton_type']), str(row_values['aton_name']), str(row_values['off_position']), str(row_values['aton_status']), str(row_values['virtual_aton']), str(row_values['channel_a']), str(row_values['channel_b']), str(row_values['tx_rx_mode']), str(row_values['power']), str(row_values['message_indicator']), str(row_values['channel_a_bandwidth']), str(row_values['channel_b_bandwidth']), str(row_values['transzone_size']), str(row_values['longitude_1']), str(row_values['latitude_1']), str(row_values['longitude_2']), str(row_values['latitude_2']), str(row_values['station_type']), str(row_values['report_interval']), str(row_values['quiet_time']), str(row_values['part_number']), str(row_values['vendor_id']), str(row_values['mother_ship_mmsi']), str(row_values['destination_indicator']), str(row_values['binary_flag']), str(row_values['gnss_status']), str(row_values['spare']), str(row_values['spare2']), str(row_values['spare3']), str(row_values['spare4'])])

    # Replace all instances of |nan| and |None| with ||. Perform in two passes to handle adjacent values.
    packed_data = packed_data.replace('|nan|','||').replace('|nan|','||')
    if packed_data.endswith('|nan'):
        packed_data = packed_data[:-3]
    packed_data = packed_data.replace('|None|','||').replace('|None|','||')
    if packed_data.endswith('|None'):
        packed_data = packed_data[:-4]

    return packed_data
