#!/usr/bin/python
# Split Postgres Export files of exactEarth position-referenced AIS data for Dr. Worm / Kristina on mmsi.
# Expects input fields:Unq_ID, mmsi, longitude, latitude, datetime, message_id, parseerror, ais_msg_eecsv
from glob import glob
import sys
import os
import time

# Flag indicating movement only records to be output.
movement_only = True

# Usage string for the script.
usage_string = "Usage: split_eE_AIS_wormdata_mvmt_on_MMSI.py outputdirectory inputfilename1 [inputfilename2 ...] \n"

# If at least two arguments are not provided, display an usage message.
if (len(sys.argv) < 3):
    print usage_string
    quit()
    
# retrieve the output directory and filename prefix
outdirectory = sys.argv[1]

# Process each input file reference passed as input.
for infile_index in range(len(sys.argv) - 2):

    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        print("Processing: " + in_filename)
        
        with open(in_filename,'r') as in_vessel_records:
        
            for line in in_vessel_records:
                
                tokenizedline = line.split(',',7)

                # Verify that the incoming line has the correct number of tokens (8).
                if(len(tokenizedline) == 8):
                    
                    mmsi = tokenizedline[1];
                    msg_type = tokenizedline[6];
                    
                    # If the message_type indicates movement, proceed.        
                    if msg_type in ('1','2','3','18','19'):
                    
                        # Attempt to open the output file.
                        try:
                            outfile = open(outdirectory  + "/" + mmsi + ".txt", 'a')
                        except IOError:
                            print "Error opening file: " + outdirectory + mmsi + "\n"
                            quit()
                            
                        if msg_type in ('1','2','3'):
                        
                            #MMSI,lon,lat,datetime,messagetype,navigational_status,sog,cog
                            pipetokenizedtoken = tokenizedline[7].split("|")
                            outline = ",".join(tokenizedline[1:6]) + "," + pipetokenizedtoken[13] + "," + pipetokenizedtoken[15] + "," + pipetokenizedtoken[19] + "\n"
                            outfile.write(outline)
                            
                        else: # msg_type in (18,19):
                        
                            #MMSI,lon,lat,datetime,messagetype,navigational_status(n/a),sog,cog
                            pipetokenizedtoken = tokenizedline[7].split("|")
                            outline = ",".join(tokenizedline[1:6]) + ",," + pipetokenizedtoken[19] + "," + pipetokenizedtoken[21] + "\n"
                            outfile.write(outline)
                            
                        outfile.close
                else:
                    
                    print("Error, incorrect number of tokens:" + str(len(tokenizedline)) + "\nLine:" + line + "\n")

