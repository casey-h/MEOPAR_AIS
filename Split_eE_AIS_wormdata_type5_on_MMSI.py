#!/usr/bin/python
# Split Postgres Export files of exactEarth position-referenced AIS data for Dr. Worm / Kristina on mmsi.

from glob import glob
import sys
import os
import time

# Flag indicating movement only records to be output.
movement_only = True

# Usage string for the script.
usage_string = "Usage: split_eE_AIS_wormdata_type5_on_MMSI.py outputdirectory inputfilename1 [inputfilename2 ...] \n"

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
                    msg_type = tokenizedline[5];
                    
                    # If the message_type indicates movement, proceed.        
                    if msg_type in ('5'):
                    
                        # Attempt to open the output file.
                        try:
                            outfile = open(outdirectory  + "/" + mmsi + ".txt", 'a')
                        except IOError:
                            print "Error opening file: " + outdirectory + mmsi + "\n"
                            quit()
                            
                        #MMSI,datetime,messagetype,vsl_name,IMO,ship_type
                        pipetokenizedtoken = tokenizedline[7].split("|")
                        outline = tokenizedline[0] + "," + tokenizedline[3] + "," + tokenizedline[4] + "," + pipetokenizedtoken[13] + "," + pipetokenizedtoken[15] + "," + pipetokenizedtoken[16] + "\n"
                        outfile.write(outline)
                            
                        outfile.close
                else:
                    
                    print("Error, incorrect number of tokens:" + str(len(tokenizedline)) + "Line:" + line + "\n")

