#!/usr/bin/python
#
# Parse_eE_AIS_PG_Exports_to_csv.py
#
# Split Postgres Export files of exactEarth  AIS data.
# Expects an input file of CSV delimited Postgres db output, containing fields (no header line):
# Unq_ID, mmsi, longitude, latitude, datetime, message_id, parseerror, ais_msg_eecsv
# 
# Example / template query:
# \copy (select Unq_ID, mmsi, longitude, latitude, datetime, message_id, parseerror, ais_msg_eecsv from ee_ais_master where mmsi in (mmsi_value_1,mmsi_value_2,mmsi_value_3) and (message_id in (msg_id_1,msg_id_2))) to '/tmp/sample_filename.txt' delimiter ','
#

from glob import glob
import sys
import os
import os.path
import time

# Usage string for the script.
usage_string = "Usage: Parse_eE_AIS_PG_Exports_to_CSV.py outputfilenameprefix inputfilename1 [inputfilename2 ...] \n"

# If at least two arguments are not provided, display an usage message.
if (len(sys.argv) < 3):
    print usage_string
    quit()
    
# retrieve the output directory and filename prefix
outputfilename = sys.argv[1]

# Process each input file reference passed as input.
for infile_index in range(len(sys.argv) - 2):

    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        print("Processing: " + in_filename)
        
        # Open the indexed filename for parsing.
        with open(in_filename,'r') as in_vessel_records:
        
            # Iterate over the input lines
            for line in in_vessel_records:
                
                tokenizedline = line.strip().split(',',7)

                # Verify that the incoming line has the correct number of tokens (8).
                if(len(tokenizedline) == 8):
                    
                    mmsi = tokenizedline[1];
                    msg_type = tokenizedline[5];
                    
                    # If the message_type indicates movement, proceed.        
                    if msg_type in ('1','2','3'):
                    
                        # Create the appropriate outfile name to match the message type.
                        output_name_1_2_3 = outputfilename + "_msg_1_2_3.csv"
                    
                        # If the output file doesn't already exist, create it and write in a header, 
                        # otherwise just open the file.
                        if (not os.path.isfile(output_name_1_2_3)):
                    
                            # Attempt to open the output file.
                            try:
                                outfile_1_2_3 = open(output_name_1_2_3, 'w')
                            except IOError:
                                print "Error opening file: " + output_name_1_2_3 + "\n"
                                quit()
                                
                            # Write out a header appropriate to the message type.
                            outfile_1_2_3.write("DB_Unq_ID,DB_mmsi,DB_lon,DB_lat,DB_datetime,DB_message_id,DB_parseerror,MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Navigational_status,ROT,SOG,Accuracy,Longitude,Latitude,COG,Heading,Maneuver,RAIM_flag,Communication_state,UTC_second,spare\n")
                            
                        else:
                        
                            # Attempt to open the output file.
                            try:
                                outfile_1_2_3 = open(output_name_1_2_3, 'a')
                            except IOError:
                                print "Error opening file: " + output_name_1_2_3 + "\n"
                                quit()

                        pipetokenizedtoken = tokenizedline[7].split("|")
                        
                        # If the incorrect number of tokens is returned for the message, display an error.
                        if (len(pipetokenizedtoken) <> 26):
                            print("Error, incorrect number of tokens for message type 1/2/3: " + str(len(pipetokenizedtoken)) + " tokens. \nLine:" + line + "\n")
                        
                        outline = ",".join(tokenizedline[0:6]) + ",".join(pipetokenizedtoken[0:11]) + ",\"" + pipetokenizedtoken[12] + "\"," + ",".join(pipetokenizedtoken[13:]) + "\n"
                        outfile_1_2_3.write(outline)
                        outfile_1_2_3.close()
                            
                    elif msg_type in ('4', '11'):
                    
                        # Create the appropriate outfile name to match the message type.
                        output_name_4_11 = outputfilename + "_msg_4_11.csv"
                    
                        # If the output file doesn't already exist, create it and write in a header, 
                        # otherwise just open the file.
                        if (not os.path.isfile(output_name_4_11)):
                    
                            # Attempt to open the output file.
                            try:
                                outfile_4_11 = open(output_name_4_11, 'w')
                            except IOError:
                                print "Error opening file: " + output_name_4_11 + "\n"
                                quit()
                                
                            # Write out a header appropriate to the message type.
                            outfile_4_11.write("DB_Unq_ID,DB_mmsi,DB_lon,DB_lat,DB_datetime,DB_message_id,DB_parseerror,MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Accuracy,Longitude,Latitude,RAIM_flag,Communication_state,UTC_year,UTC_month,UTC_day,UTC_hour,UTC_minute,UTC_second,Fixing_device,Transmission_control,spare\n")
                            
                        else:
                        
                            # Attempt to open the output file.
                            try:
                                outfile_4_11 = open(output_name_4_11, 'a')
                            except IOError:
                                print "Error opening file: " + output_name_4_11 + "\n"
                                quit()

                        pipetokenizedtoken = tokenizedline[7].split("|")
                        
                        # If the incorrect number of tokens is returned for the message, display an error.
                        if (len(pipetokenizedtoken) <> 27):
                            print("Error, incorrect number of tokens for message type 4/11: " + str(len(pipetokenizedtoken)) + " tokens. \nLine:" + line + "\n")
                        
                        outline = ",".join(tokenizedline[0:6]) + ",".join(pipetokenizedtoken[0:11]) + ",\"" + pipetokenizedtoken[12] + "\"," + ",".join(pipetokenizedtoken[13:]) + "\n"
                        outfile_4_11.write(outline)
                        outfile_4_11.close()
                        
                    elif msg_type in ('5'):

                        # Create the appropriate outfile name to match the message type.
                        output_name_5 = outputfilename + "_msg_5.csv"
                    
                        # If the output file doesn't already exist, create it and write in a header, 
                        # otherwise just open the file.
                        if (not os.path.isfile(output_name_5)):
                    
                            # Attempt to open the output file.
                            try:
                                outfile_5 = open(output_name_5, 'w')
                            except IOError:
                                print "Error opening file: " + output_name_5 + "\n"
                                quit()
                                
                            # Write out a header appropriate to the message type.
                            outfile_5.write("DB_Unq_ID,DB_mmsi,DB_lon,DB_lat,DB_datetime,DB_message_id,DB_parseerror,MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Vessel_Name,Call_sign,IMO,Ship_Type,Dimension_to_Bow,Dimension_to_stern,Dimension_to_port,Dimension_to_starboard,Draught,Destination,AIS_version,Fixing_device,Transmission_control,ETA_month,ETA_day,ETA_hour,ETA_minute,Sequence,Data_terminal,Mode,spare,spare2\n")
                            
                        else:
                        
                            # Attempt to open the output file.
                            try:
                                outfile_5 = open(output_name_5, 'a')
                            except IOError:
                                print "Error opening file: " + output_name_5 + "\n"
                                quit()

                        pipetokenizedtoken = tokenizedline[7].split("|")
                        
                        # If the incorrect number of tokens is returned for the message, display an error.
                        if (len(pipetokenizedtoken) <> 35):
                            print("Error, incorrect number of tokens for message type 5: " + str(len(pipetokenizedtoken)) + " tokens. \nLine:" + line + "\n")
                        
                        outline = ",".join(tokenizedline[0:6]) + ",".join(pipetokenizedtoken[0:11]) + ",\"" + pipetokenizedtoken[12] + "\",\"" + pipetokenizedtoken[13] + "\"," + ",".join(pipetokenizedtoken[14:]) + "\n"
                        
                        # Need to wrap 'Destination' as well !!!
                        
                        outfile_5.write(outline)
                        outfile_5.close()
                        
                    elif msg_type in ('18', '19'):
                        
                        # Create the appropriate outfile name to match the message type.
                        output_name_18_19 = outputfilename + "_msg_18_19.csv"
                    
                        # If the output file doesn't already exist, create it and write in a header, 
                        # otherwise just open the file.
                        if (not os.path.isfile(output_name_18_19)):
                    
                            # Attempt to open the output file.
                            try:
                                outfile_18_19 = open(output_name_18_19, 'w')
                            except IOError:
                                print "Error opening file: " + output_name_18_19 + "\n"
                                quit()
                                
                            # Write out a header appropriate to the message type.
                            outfile_18_19.write("DB_Unq_ID,DB_mmsi,DB_lon,DB_lat,DB_datetime,DB_message_id,DB_parseerror,MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Vessel_Name,Ship_Type,Dimension_to_Bow,Dimension_to_stern,Dimension_to_port,Dimension_to_starboard,SOG,Accuracy,Longitude,Latitude,COG,Heading,Regional,RAIM_flag,Communication_flag,Communication_state,UTC_second,Fixing_device,Data_terminal,Mode,unit_flag,display,DSC,band,msg22,spare,spare2\n")
                            
                        else:
                        
                            # Attempt to open the output file.
                            try:
                                outfile_18_19 = open(output_name_18_19, 'a')
                            except IOError:
                                print "Error opening file: " + output_name_18_19 + "\n"
                                quit()

                        pipetokenizedtoken = tokenizedline[7].split("|")
                        
                        # If the incorrect number of tokens is returned for the message, display an error.
                        if (len(pipetokenizedtoken) <> 40):
                            print("Error, incorrect number of tokens for message type 18/19: " + str(len(pipetokenizedtoken)) + " tokens. \nLine:" + line + "\n")
                        
                        outline = ",".join(tokenizedline[0:6]) + ",".join(pipetokenizedtoken[0:11]) + ",\"" + pipetokenizedtoken[12] + "\",\"" + pipetokenizedtoken[13] + "\"," + ",".join(pipetokenizedtoken[14:]) + "\n"
                        outfile_18_19.write(outline)
                        outfile_18_19.close()
                else:
                    
                    print("Error, incorrect number of base tokens from DB record:" + str(len(tokenizedline)) + " tokens. \nLine:" + line + "\n")