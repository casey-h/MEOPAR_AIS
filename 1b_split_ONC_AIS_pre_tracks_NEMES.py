#!/usr/bin/python
# Split pre-parsed ONC-provided position-referenced AIS data (parsed using 
# variation on ais.py from GPSD project) into basic movement data files on 
# message type group.

from glob import glob
import sys
import os
import time

# Function (ins_number) to determine if a string represents a number (specifically, an mmsi, code from: http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python).
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
# End function (is_number)

# Usage string for the script.
usage_string = "Usage: split_ONC_AIS_pre_tracks_NEMES.py outputdirectory outputfilenameprefix inputfilename1 [inputfilename2 ...] \n\nSplits pre-parsed position-referenced AIS records, as provided by ONC, parsed by 0_gpsd_ais_ONC_Parsing.py into sub files by message type, while re-ordering the incoming data to match the format expected by the track division script, 2_divide_tracks_v_NEMES.py. Script created for NEMES project.\n"

# If at least two arguments are not provided, display an usage message.
if (len(sys.argv) < 4):
    print usage_string
    quit()
    
# retrieve the output directory and filename prefix
outdirectory = sys.argv[1]
out_filename_prefix = sys.argv[2]

# Define an array of output filenames, based on the provided prefix, to store the parsed results.
out_filename_array = [out_filename_prefix + "_1_2_3.csv", out_filename_prefix + "_5.csv", out_filename_prefix + "_18_19.csv", out_filename_prefix + "_other.csv"]

# Check each potential output file for existence before running.
for outfile_index in range(len(out_filename_array) - 1):
    if os.path.exists(out_filename_array[outfile_index]):
        print "Error, output file exists: (" + out_filename_array[outfile_index] +  ") aborting."
        quit()
        
# Open all output files required.
out_message_records = []

for outfile_index in range(len(out_filename_array)):
    try:
        #out_message_records[outfile_index] = open(out_filename_array[outfile_index], 'w')
        out_message_records.append(open(out_filename_array[outfile_index], 'w'))
    except IOError:
        print "Error opening output file: " + out_filename_array[outfile_index] + "\n"
        quit()

# Print a header line for each of the message type groups to be extracted from the eE AIS data.

#1_2_3
out_message_records[0].write("ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc\n");

#5
out_message_records[1].write("ext_timestamp,msgid,repeat,mmsi,ais_ver,imo,call_sign,vsl_name,ship_type,dim_bow,dim_ster,dim_port,dim_star,pos_fix,eta_min,eta_day,eta_hour,eta_min,draught,dest,dte\n"
);

#18_19 
out_message_records[2].write("ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc\n");
   
#other
out_message_records[3].write("Field set depends on message type, see 0_gpsd_ais_ONC_parsing.py \n")

# Process each input file reference passed as input.
for infile_index in range(len(sys.argv) - 3):

    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(3 + infile_index)]):
    
        print("Processing: " + in_filename)
        
        with open(in_filename,'r') as in_vessel_records:
        
            # Reset a counter into the input file.
            in_line_counter = 0
        
            for line in in_vessel_records:
                
                # Tokenize the record data on the basis of pipe-character.
                pipetokenizedline = line.strip().split('|')
                
                # Copy the type value of the input message.
                input_msg_type = pipetokenizedline[1]
                
                # Output tokenized fields according to the message type observed.
                #1_2_3                
                if(input_msg_type in ("1", "2", "3")):

                    # Extract the date in "yyyymmddThhnnss.sssZ" format and convert to %Y%m%d_%H%M%S 
                    # format, rounding the seconds value to an whole digit.
                    in_date = pipetokenizedline[0]
                    out_date = in_date[0:8] + "_" + in_date[9:13] + "%02d" % (int(round(float(in_date[13:19]))))

                                
                    # ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc
                    # ONC Parsed: 0,1,3,4,6,10,11,9,8,7
                    out_message_records[0].write(out_date + "," + pipetokenizedline[1] + "," + pipetokenizedline[3] + "," + pipetokenizedline[4] + "," + pipetokenizedline[6] + "," + pipetokenizedline[10] + "," + pipetokenizedline[11] + "," + pipetokenizedline[9] + "," + pipetokenizedline[8] + "," + pipetokenizedline[7] + "\n")

                #5 - Write all tokens.
                elif(input_msg_type in ("5")):

                    # Extract the date in "yyyymmddThhnnss.sssZ" format and convert to %Y%m%d_%H%M%S 
                    # format, rounding the seconds value to an whole digit.
                    in_date = pipetokenizedline[0]
                    out_date = in_date[0:8] + "_" + in_date[9:13] + "%02d" % (int(round(float(in_date[13:19]))))
                
                    out_message_records[1].write(out_date + "|" + "|".join(pipetokenizedline[1:]))

                #18_19 
                elif(input_msg_type in ("18", "19")):

                    # Extract the date in "yyyymmddThhnnss.sssZ" format and convert to %Y%m%d_%H%M%S 
                    # format, rounding the seconds value to an whole digit.
                    in_date = pipetokenizedline[0]
                    out_date = in_date[0:8] + "_" + in_date[9:13] + "%02d" % (int(round(float(in_date[13:19]))))

                    #ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc
                    # ONC Parsed:  0,1,3,N/A,5,9,10,8,7,6
                    out_message_records[2].write(out_date + "," + pipetokenizedline[1] + "," + pipetokenizedline[3] + "," + "" + "," + pipetokenizedline[5] + "," 
                    + pipetokenizedline[9] + "," + pipetokenizedline[10] + "," + pipetokenizedline[8] + "," + pipetokenizedline[7] + "," + pipetokenizedline[6] + "," + "\n")
                    
                #other - Write all tokens.
                else:
                
                    # Extract the date in "yyyymmddThhnnss.sssZ" format and convert to %Y%m%d_%H%M%S 
                    # format, rounding the seconds value to an whole digit.
                    in_date = pipetokenizedline[0]
                    out_date = in_date[0:8] + "_" + in_date[9:13] + "%02d" % (int(round(float(in_date[13:19]))))
                
                    out_message_records[3].write(out_date + "|" + "|".join(pipetokenizedline[1:]))

                    
                # Increment the current input line counter.
                in_line_counter += 1
                    
# Close / flush all output files required.
for outfile_index in range(len(out_message_records)):  
    out_message_records[outfile_index].close()
    
# Run through the 1,2,3 and 18,19 parsed files, allocating the records within to 
# new output files on the basis of mmsi.
for data_index in {0,2}:
    
    print("Processing: " + out_filename_array[(data_index)])

    with open(out_filename_array[data_index],'r') as in_parsed_AIS_underway:

        for line in in_parsed_AIS_underway:
            
            tokenizedline = line.split(',')

            # Verify that the incoming line has the correct number of tokens.
            if(len(tokenizedline) > 8):
                
                mmsi = tokenizedline[2];
                
                # If the mmsi token is numeric, use its value as the output filename.
                if is_number(mmsi):
                
                    try:
                        outfile = open(outdirectory  + "/" + mmsi + ".txt", 'a')
                    except IOError:
                        print "Error opening file MMSI ->: " + outdirectory + mmsi + "\n"
                        continue
                        
                    outfile.write(line)
                    outfile.close()

                        
                # If the mmsi token is non-numeric, write the output to an "other" file.
                else:
                    try:
                        outfile = open(outdirectory  + "/other.txt", 'a')
                    except IOError:
                        print "Error opening \"other\" output file.\n"
                        continue
                    
                    outfile.write(line)
                    outfile.close()
            else:
                
                print("Error, incorrect number of tokens:" + str(len(tokenizedline)) + "Line:" + line + "\n")