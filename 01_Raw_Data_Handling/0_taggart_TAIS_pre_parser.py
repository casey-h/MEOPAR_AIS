#!/usr/bin/python
#
# 0c_Taggart_TAIS_pre_parser.py - A pre-parser for *.raw files of 
# Terrestrial AIS data, sourced from Dr. Chris Taggart's East Coast 
# network of receivers. This script iterates over a number of input 
# files, concatenating them into a single output, while prepending 
# date values to NMEA lines and dropping empty lines. The output 
# should be suitable for subsequent parsing via 0_gpsd_ais_NM4_parsing.py
#

# CH20171205 Added parentheses to print calls for Python3 compatibility.

from glob import glob 
import os, sys
from datetime import datetime, timedelta
from pytz import timezone
import pytz

usage_msg = ("\nUsage: 0c_Taggart_TAIS_pre_parser.py outfilename infile1 {infile2, infile3, ...}\n"
            "Where outfilename is the path to the output file under which the concatenated results "
            "will be stored, and infile1 ... are the *.raw input files of T-AIS data to be concatenated."
)

# Establish the timezones to be used.
utctz = pytz.utc
localtz = timezone('America/Halifax')

# If at least two arguments are not provided, display an usage message.
if (len(sys.argv) < 3):
    print (usage_msg)
    quit()
    
# retrieve the output directory and filename prefix
out_filename = sys.argv[1]

# Check the output file for existence before running.
if os.path.exists(out_filename):
    print ("Error, output file exists: (" + out_filename +  ") aborting.")
    quit()
        
# Open the output file.
with open(out_filename,'w') as outfile:

    # Process each input file reference passed as input.
    for infile_index in range(len(sys.argv) - 2):

        # Attempt wildcard expansion on any input file specified.
        for in_filename in glob(sys.argv[(2 + infile_index)]):
        
            print ("Processing: " + in_filename)
            
            # Open the incoming filename.
            with open(in_filename,'r') as in_T_AIS_records:
            
                # Reset a counter into the input file.
                in_line_counter = 0
            
                # Initialize a date string to be stamped across output NMEA lines.
                curr_date_text = ""
            
                # Iterate over the lines in the incoming records.
                for line in in_T_AIS_records:
                
                    # Strip any leading / trailing whitespace characters from 
                    # the input line.
                    strip_line = line.strip()
                
                    # If the line is empty, skip it.
                    if (strip_line == ""):
                        continue
                        
                    # If the line is a date header, copy its value for the 
                    # succeeding line.
                    elif (strip_line[-5:] == "data:"):
                    
                        # Extract the date portion of the line, and formate as a datetime object.
                        curr_date_text = strip_line[:-5].strip()
                        formatted_local_date = datetime.strptime(curr_date_text, "%d,%m,%y,%H,%M,%S,%Z")
                        
                        # Interpret the date as local to America/Halifax (sic) 
                        # including provision for daylight savings.
                        formatted_utc_date = localtz.localize(formatted_local_date).astimezone(utctz)
                        
                    # If the line is not empty, nor a date, copy it to output, 
                    # with the data header as a prefix.
                    else:
                    
                        # Original -- Carry original date through.
                        #outfile.write(curr_date_text + " " + strip_line + "\n")
                        # Output the UTC date, reformatted as: yyyymmddTHHMMSS.000Z
                        outfile.write(formatted_utc_date.strftime("%Y%m%dT%H%M%S") + ".000Z " + strip_line + "\n")
