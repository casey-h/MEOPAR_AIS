#!/usr/bin/python
#
# 0c_CSA_TAIS_pre_parser.py - A pre-parser for *.log files of 
# Terrestrial AIS data, sourced from CSA via Array Inc.
# This script iterates over a number of input 
# files, concatenating them into a single output, while prepending 
# date values to NMEA lines and dropping empty lines. The output 
# should be suitable for subsequent parsing via 0_gpsd_ais_NM4_parsing.py
#

from glob import glob 
import os, sys
from datetime import datetime

usage_msg = ("\nUsage: 0c_CSA_TAIS_pre_parser.py outfilename infile1 {infile2, infile3, ...}\n"
            "Where outfilename is the path to the output file under which the concatenated results "
            "will be stored, and infile1 ... are the *.raw input files of T-AIS data to be concatenated."
)

# If at least two arguments are not provided, display an usage message.
if (len(sys.argv) < 3):
    print usage_msg
    quit()
    
# retrieve the output directory and filename prefix
out_filename = sys.argv[1]

# Check the output file for existence before running.
if os.path.exists(out_filename):
    print "Error, output file exists: (" + out_filename +  ") aborting."
    quit()
        
# Open the output file.
with open(out_filename,'w') as outfile:

    # Process each input file reference passed as input.
    for infile_index in range(len(sys.argv) - 2):

        # Attempt wildcard expansion on any input file specified.
        for in_filename in glob(sys.argv[(2 + infile_index)]):
        
            print("Processing: " + in_filename)
            
            # Open the incoming filename.
            with open(in_filename,'r') as in_T_AIS_records:
            
                # Reset a counter into the input file.
                in_line_counter = 0
            
                # Initialize a date string to be stampted across output NMEA lines.
                curr_date_text = ""
            
                # Iterate over the lines in the incoming records.
                for line in in_T_AIS_records:
                
                    # Strip any leading / trailing whitespace characters from 
                    # the input line.
                    strip_line = line.strip()
                
                    # If the line is empty, skip it.
                    if (strip_line == ""):
                        continue
                    
                    # Determine the number of delimiters (commas) in the incoming data line.
                    delimiter_count = strip_line.count(",")
                        
                    # If the line doesn't contain the correct number of delimiters to be a data line, skip it.
                    if (delimiter_count < 7) or (delimiter_count > 8):
                        continue
                        
                    # If the line has a single trailing token, presume 
                    # it to be the date value, and prepend it to the NMEA 
                    # message on output.
                    elif delimiter_count == 7:

                        split_line = strip_line.split(",")
                        datestamp = datetime.utcfromtimestamp(float(split_line[7]))
                        outfile.write(datestamp.strftime("%Y%m%dT%H%M%S") + ".000Z " + ",".join(split_line[0:7]) + "\n")
                    
                    # If the line has a two trailing tokens (8 total), 
                    # presume the final token to be the date value, and 
                    # prepend it to the NMEA message on output. Discard 
                    # the other  token.
                    else:

                        split_line = strip_line.split(",")
                        datestamp = datetime.utcfromtimestamp(float(split_line[8]))
                        outfile.write(datestamp.strftime("%Y%m%dT%H%M%S") + ".000Z " + ",".join(split_line[0:7]) + "\n")

