#!/usr/bin/python
# Parses out 5 fields from eE formatted AIS data (MMSI, Lat, Lon, Date and Message Type), attaches a Unique ID based  
# on the source file and the row number, an indication as to whether or not there was any problem in parsing the 5 AIS
# fields and, finally, the original CSV row. The output is tab delimited, and any characters with special meaning in
# the Postgres \copy command (i.e. \) are escaped. The data output for the CSV row are restricted to those fields 
# appropriate for the message type, and excludes any fields not cited in the AIS definition for the indicated type. 
# The fields are pipe-delimited rather than the original comma separated, double quote enclosed, format. Rows for 
# which the message type is not available or incorrect are output with all field values.

from glob import glob
import sys
import os
import re

# Stackoverflow sourced short f'n for testing whether / not string values are numeric, modified from 
# source to account for multiple number types. 
# Type-value ranges for PG: http://www.postgresql.org/docs/9.1/static/datatype-numeric.html
def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
        
def is_integer(s):
    try:
        inttest = int(s)
        #Ignores possibility of -2147483648, considered acceptable compromise for performance, value 
        #shouldn't exist in AIS data.
        if abs(inttest) < 2147483648:
            return True
        else:
            return False
    except ValueError:
        return False
       
# Usage string for the script.
# USAGE_STRING = "Usage: split_eE_AIS_msg_type.py outputfilenameprefix inputfilename1 [inputfilename2 ...] \n\nSplits eE AIS records of a single message type group from a group of input files and stores the result in a single file per message type group.\n"
USAGE_STRING = "Usage: split_eE_AIS_msg_type.py [S|T] outputfilename inputfilename1 [inputfilename2 ...] \n\nParses out 5 basic fields (MMSI, Type, Lat, Lon, Date) from eE formatted AIS data, generates a unique line ID. Also tests that the basic fields parse properly. Inserts the 7 generated fields (5 + ID, Flag as result of parse test) along with the original line, all in a tab delimited output file. Uses the S (Satellite) / T (Terrestrial) designation to aid in generating the appropriate unique ID, along with the date from the input filename. Presumes files in the format 'exactEarth_historical_data_YYYY-MM-DD.csv' or 'exactEarth_historical_data_YYYYMMDD.csv'\n."

# Array of message types with positional information.
POSITIONAL_MESSAGE_TYPES = [1, 2, 3, 4, 11, 9, 17, 18, 19, 21, 27]

# If at least four arguments are not provided, display an usage message.
if (len(sys.argv) < 4):
    print USAGE_STRING
    quit()

# retrieve the unique row id prefix for Satellite (sat) or Terrestrial (ter)
source_prefix = sys.argv[1]

# If the data source is not properly specified, display an error message and the usage string before aborting.
if(not (source_prefix in ("S","T"))):
    print "Error, data source indicator must be either S for Satellite or T for Terrestrial."
    print USAGE_STRING
    quit()
    
# retrieve the output filename.
out_filename = sys.argv[2]

# Check the output file for existence before running.
if os.path.exists(out_filename):
    print "Error, output file exists: (" + out_filename +  ") aborting."
    quit()
        
# Open the output file.
try:

    out_records = open(out_filename, 'w')
    
except IOError:
    print "Error opening output file: " + out_filename + "\n"
    quit()

# Print a header line for each of the output files to be generated from the eE AIS data.
# Do not write out a header line, gets in the way of \copy - out_records.write("Unq_ID\tMMSI\tLongitude\tLatitude\tDate\tMsgType\tParseError\tAIS_CSV\n")

# Process each input file reference passed as input.
for infile_index in range(len(sys.argv) - 3):
    
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(3 + infile_index)]):
    
        print("Processing: " + in_filename)
        
        with open(in_filename,'r') as in_vessel_records:
        
            # Calculate the length of the input filename string.
            in_filename_len = len(in_filename)
        
            # Calculate a unique ID prefix value based on the input filename. Prepend the source prefix (S or T), plus 'E' to indicate exactEarth.
            # If there are dashes in the input filename, presume the format as: exactEarth_historical_data_YYYY-MM-DD.csv
            if(in_filename.find("-") > -1):

                unq_ID_prefix = source_prefix + 'E' + in_filename[in_filename_len-14:in_filename_len-4].replace("-","") + "_"
            
            # If there are no dashes in the input filename, presume the format as: exactEarth_historical_data_YYYYMMDD.csv
            else:
            
                unq_ID_prefix = source_prefix + 'E' +  in_filename[in_filename_len-12:in_filename_len-4] + "_"
            
            #CCCCC
            print "unq_ID_prefix: " + unq_ID_prefix 
        
            # Reset a counter into the input file.
            in_line_counter = 0
        
            for line in in_vessel_records:
                
                # If the current line is the first of the file, presume that it contains the header and skip the iteration.
                if (in_line_counter == 0):
                    in_line_counter += 1
                    continue

                # Drop heading and trailing double quotes, as well as trailing CRLF from the input string, then tokenize on '","'.
                stripline = line[1:-3]
                
                # Split the input line only up to one token past the last value required (latitude @ 29 splits, so split to 30)
                tokenizedline = stripline.split('","')
                
                # Initialize a flag indicating whether or not the base fields from record were found to be parseable.
                parse_error_flag = False
                
                # If less than 30 tokens are noted, set the parse error, and insert the unique id along with null fields for the calculations, as at least one of the
                # 'critical' fields will be omitted. Output the entire input line to the last field.
                if(len(tokenizedline) < 30):
                
                    print "Critical parse error, unable to parse MMSI / Msg Type / Lat / Lon from input line.\n Line: " + stripline
                    parse_error_flag = True
                    PG_safe_line = stripline.replace("\\","\\\\")
                    out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line)
                    
                #If the correct number of tokens is noted, continue processing.
                else:
                    # Obtain the string containing the message type.
                    str_msg_type = tokenizedline[1]
     
                    # Attempt to obtain the message type as an integer from the second token returned by the split operation.
                    try:
                        input_msg_type = int(str_msg_type)
                        
                        #If the message type is not in the expected range (1-27), update the value to null and set the parse error flag for the row.
                        if(not ((input_msg_type > 0) and (input_msg_type < 28))):

                            #CCC Debug
                            print "Message type parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + str_msg_type + ")\n"
                            
                            str_msg_type  = "\N"
                            parse_error_flag = True

                            
                    # If the value for message type cannot be parsed into an integer, set the value to null and set the parse error flag for the row.
                    except ValueError:
                    
                        #CCC Debug
                        print "Message type parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + str_msg_type + ")"
                    
                        str_msg_type = "\N"
                        parse_error_flag = True

                    # Also, If the message type is not known, then latitude and longitude values may or may not be present; 
                    # they should be parsed if available, but no error thrown if they are not present.
                    if (str_msg_type == "\N"):
                    
                        longitude_string = tokenizedline[28]
                        latitude_string = tokenizedline[29]
                        
                        # If either of the coordinates are not parseable as floating point numbers, check to see if they're just 
                        # improperly formatted exponenets (e.g. "1.0E2.0" -- trailing .0 is superfluous and wrong {unless the 
                        # system supports fractional powers of 10}, which python doesn't) -- either fix the value, or set it to 
                        # null.
                        if (not(is_float(longitude_string))): 
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            if(suffix_search is None):
                                longitude_string = "\N"
                            else:
                                print "Translating: " + longitude_string + " to: " + suffix_search.group(1)
                                longitude_string = suffix_search.group(1)
                        
                        if (not(is_float(latitude_string))):
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            if(suffix_search is None):
                                latitude_string = "\N"
                            else:
                                print "Translating: " + latitude_string + " to: " + suffix_search.group(1)
                                latitude_string = suffix_search.group(1)

                    # If the message type suggests that longitude and latitude fields should be present, verify that the values are actually coordinates.            
                    elif(input_msg_type in POSITIONAL_MESSAGE_TYPES):
                    
                        longitude_string = tokenizedline[28]
                        latitude_string = tokenizedline[29]
                        
                        # If either of the coordinates are not parseable as floating point numbers, check to see if they're just 
                        # improperly formatted exponenets (e.g. "1.0E2.0" -- trailing .0 is superfluous and wrong {unless the 
                        # system supports fractional powers of 10}, which python doesn't) -- either fix the value, or set it to 
                        # null.
                        if (not(is_float(longitude_string))): 
                        
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            if(suffix_search is None):
                                #CCC Debug
                                print "Longitude parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + longitude_string + ")"
                            
                                longitude_string = "\N"
                                parse_error_flag = True
                            else:
                                print "Translating: " + longitude_string + " to: " + suffix_search.group(1)
                                longitude_string = suffix_search.group(1)

                        if (not(is_float(latitude_string))):
                        
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            if(suffix_search is None):
                                #CCC Debug
                                print "Latitude parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + latitude_string + ")"

                                latitude_string = "\N"
                                parse_error_flag = True
                            else:
                                print "Translating: " + latitude_string + " to: " + suffix_search.group(1)
                                latitude_string = suffix_search.group(1)

                    # Attempt to parse coordinate values for all non-positional message types, but do not flag errors when 
                    # coordinates are not defined.
                    else:
                    
                        longitude_string = tokenizedline[28]
                        latitude_string = tokenizedline[29]
                        
                        # If either of the coordinates are not parseable as floating point numbers, check to see if they're just 
                        # improperly formatted exponenets (e.g. "1.0E2.0" -- trailing .0 is superfluous and wrong {unless the 
                        # system supports fractional powers of 10}, which python doesn't) -- either fix the value, or set it to 
                        # null.
                        if (not(is_float(longitude_string))): 
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',longitude_string)
                            if(suffix_search is None):
                                longitude_string = "\N"
                            else:
                                print "Translating: " + longitude_string + " to: " + suffix_search.group(1)
                                longitude_string = suffix_search.group(1)
                        
                        if (not(is_float(latitude_string))):
                            #suffix_search = re.search('([0-9\.]+E[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',latitude_string)
                            
                            if(suffix_search is None):
                                latitude_string = "\N"
                            else:
                                print "Translating: " + latitude_string + " to: " + suffix_search.group(1)
                                latitude_string = suffix_search.group(1)
                    
                    # If the date value is not of the expected length, or if it is, but has unexpected non-numeric components, set the parse error 
                    #flag and insert a null in place of the date.
                    raw_date_string = tokenizedline[3]

                    if(len(raw_date_string) != 15):
                        #CCC Debug
                        print "Date string parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + raw_date_string + ")"
                    
                        parse_error_flag = True
                        parsed_date_string = "\N"
                        
                    elif (not(is_integer(raw_date_string[0:8])) or not(is_integer(raw_date_string[10:16]))):
                        #CCC Debug
                        print "Date string parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + raw_date_string + ")"
                        
                        parse_error_flag = True
                        parsed_date_string = "\N"
                        
                    # If the date is ok, construct a Postgres-acceptable timestamp from the date_string value.
                    #e.g 20141001_000005 -> 2014-10-01 00:00:05                 
                    else:
                        parsed_date_string = raw_date_string[0:4] + "-" + raw_date_string[4:6] + "-" + raw_date_string[6:8] + " " + raw_date_string[9:11] + ":" + raw_date_string[11:13] + ":" + raw_date_string[13:15]

                    # If the MMSI is non numeric, set the parse error flag and insert a null in place of the MMSI.
                    MMSI_string = tokenizedline[0]
                    if(not(is_integer(MMSI_string))):
                    
                        #CCC Debug
                        print "MMSI parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + MMSI_string + ")"
                        
                        parse_error_flag = True
                        MMSI_string = "\N"
                        
                    ######################
                    # Output tokenized raw fields according to the message type observed, escape any backslashes in the input line.
                    #1_2_3                
                    if(input_msg_type in (1, 2, 3)):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[24] + "|" + tokenizedline[25] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[31] + "|" + tokenizedline[33] + "|" + tokenizedline[34] + "|" + tokenizedline[36] + "|" + tokenizedline[42] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #4_11 
                    elif(input_msg_type in (4, 11)):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[34] + "|" + tokenizedline[36] + "|" + tokenizedline[37] + "|" + tokenizedline[38] + "|" + tokenizedline[39] + "|" + tokenizedline[40] + "|" + tokenizedline[41] + "|" + tokenizedline[42] + "|" + tokenizedline[43] + "|" + tokenizedline[44] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #5  
                    elif(input_msg_type == 5):
                    
                        if(len(tokenizedline) < 136):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[21] + "|" + tokenizedline[22] + "|" + tokenizedline[23] + "|" + tokenizedline[43] + "|" + tokenizedline[44] + "|" + tokenizedline[45] + "|" + tokenizedline[46] + "|" + tokenizedline[47] + "|" + tokenizedline[48] + "|" + tokenizedline[49] + "|" + tokenizedline[65] + "|" + tokenizedline[67] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "\n").replace("\\","\\\\")

                    #6 
                    elif(input_msg_type == 6):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                        
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[49] + "|" + tokenizedline[50] + "|" + tokenizedline[51] + "|" + tokenizedline[52] + "|" + tokenizedline[53] + "|" + tokenizedline[54] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                        
                    #7_13
                    elif(input_msg_type in (7, 13)):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[55] + "|" + tokenizedline[56] + "|" + tokenizedline[57] + "|" + tokenizedline[58] + "|" + tokenizedline[59] + "|" + tokenizedline[60] + "|" + tokenizedline[61] + "|" + tokenizedline[62] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                    
                    #8 
                    elif(input_msg_type == 8):
                    
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                        
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[52] + "|" + tokenizedline[53] + "|" + tokenizedline[54] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #9 
                    elif(input_msg_type == 9):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[34] + "|" + tokenizedline[35] + "|" + tokenizedline[36] + "|" + tokenizedline[42] + "|" + tokenizedline[63] + "|" + tokenizedline[64] + "|" + tokenizedline[65] + "|" + tokenizedline[66] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #10 
                    elif(input_msg_type == 10):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                        
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[32] + "|" + tokenizedline[50] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #12 
                    elif(input_msg_type == 12):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[49] + "|" + tokenizedline[50] + "|" + tokenizedline[51] + "|" + tokenizedline[67] + "|" + tokenizedline[68] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #14 
                    elif(input_msg_type == 14):
                    
                        if(len(tokenizedline) < 136):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[67] + "|" + tokenizedline[68] + "|" + tokenizedline[69] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "\n").replace("\\","\\\\")
                        
                    #15
                    elif(input_msg_type == 15):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[55] + "|" + tokenizedline[57] + "|" + tokenizedline[71] + "|" + tokenizedline[72] + "|" + tokenizedline[73] + "|" + tokenizedline[74] + "|" + tokenizedline[75] + "|" + tokenizedline[76] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
     
                    #16
                    elif(input_msg_type == 16):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[77] + "|" + tokenizedline[78] + "|" + tokenizedline[79] + "|" + tokenizedline[80] + "|" + tokenizedline[81] + "|" + tokenizedline[82] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                    
                    #17 
                    elif(input_msg_type == 17):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[32] + "|" + tokenizedline[54] + "|" + tokenizedline[83] + "|" + tokenizedline[84] + "|" + tokenizedline[85] + "|" + tokenizedline[86] + "|" + tokenizedline[87] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #18_19 
                    elif(input_msg_type in (18, 19)):
                    
                        if(len(tokenizedline) < 136):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[13] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[31] + "|" + tokenizedline[32] + "|" + tokenizedline[34] + "|" + tokenizedline[35] + "|" + tokenizedline[36] + "|" + tokenizedline[42] + "|" + tokenizedline[43] + "|" + tokenizedline[65] + "|" + tokenizedline[66] + "|" + tokenizedline[88] + "|" + tokenizedline[89] + "|" + tokenizedline[90] + "|" + tokenizedline[91] + "|" + tokenizedline[92] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "\n").replace("\\","\\\\")
                        
                    #20
                    elif(input_msg_type == 20):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[93] + "|" + tokenizedline[94] + "|" + tokenizedline[95] + "|" + tokenizedline[96] + "|" + tokenizedline[97] + "|" + tokenizedline[98] + "|" + tokenizedline[99] + "|" + tokenizedline[100] + "|" + tokenizedline[101] + "|" + tokenizedline[102] + "|" + tokenizedline[103] + "|" + tokenizedline[104] + "|" + tokenizedline[105] + "|" + tokenizedline[106] + "|" + tokenizedline[107] + "|" + tokenizedline[108] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                    
                    #21 
                    elif(input_msg_type == 21):
                    
                        if(len(tokenizedline) < 136):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[34] + "|" + tokenizedline[42] + "|" + tokenizedline[43] + "|" + tokenizedline[66] + "|" + tokenizedline[69] + "|" + tokenizedline[70] + "|" + tokenizedline[109] + "|" + tokenizedline[110] + "|" + tokenizedline[111] + "|" + tokenizedline[112] + "|" + tokenizedline[113] + "|" + tokenizedline[114] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "\n").replace("\\","\\\\")
                        
                    #22
                    elif(input_msg_type == 22):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[114] + "|" + tokenizedline[115] + "|" + tokenizedline[116] + "|" + tokenizedline[117] + "|" + tokenizedline[118] + "|" + tokenizedline[119] + "|" + tokenizedline[120] + "|" + tokenizedline[121] + "|" + tokenizedline[122] + "|" + tokenizedline[123] + "|" + tokenizedline[124] + "|" + tokenizedline[125] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #23
                    elif(input_msg_type == 23):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[16] + "|" + tokenizedline[116] + "|" + tokenizedline[122] + "|" + tokenizedline[123] + "|" + tokenizedline[124] + "|" + tokenizedline[125] + "|" + tokenizedline[126] + "|" + tokenizedline[127] + "|" + tokenizedline[128] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                    
                    #24 
                    elif(input_msg_type == 24):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[43] + "|" + tokenizedline[129] + "|" + tokenizedline[130] + "|" + tokenizedline[131] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #25 
                    elif(input_msg_type == 25):
                    
                        if(len(tokenizedline) < 133):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[50] + "|" + tokenizedline[52] + "|" + tokenizedline[53] + "|" + tokenizedline[54] + "|" + tokenizedline[132] + "|" + tokenizedline[133] + "\n").replace("\\","\\\\")

                    #26 
                    elif(input_msg_type == 26):
                    
                        if(len(tokenizedline) < 133):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[35] + "|" + tokenizedline[36] + "|" + tokenizedline[50] + "|" + tokenizedline[52] + "|" + tokenizedline[53] + "|" + tokenizedline[54] + "|" + tokenizedline[132] + "|" + tokenizedline[133] + "\n").replace("\\","\\\\")

                    #27 
                    elif(input_msg_type == 27):
                    
                        if(len(tokenizedline) < 135):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[24] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[34] + "|" + tokenizedline[134] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")

                    #other
                    else:
                    
                        if(len(tokenizedline) < 138):
                    
                            print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + stripline
                            parse_error_flag = True
                            PG_safe_line = stripline.replace("\\","\\\\")
                            out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                            continue
                            
                        else:
                    
                            PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[21] + "|" + tokenizedline[22] + "|" + tokenizedline[23] + "|" + tokenizedline[24] + "|" + tokenizedline[25] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[31] + "|" + tokenizedline[32] + "|" + tokenizedline[33] + "|" + tokenizedline[34] + "|" + tokenizedline[35] + "|" + tokenizedline[36] + "|" + tokenizedline[37] + "|" + tokenizedline[38] + "|" + tokenizedline[39] + "|" + tokenizedline[40] + "|" + tokenizedline[41] + "|" + tokenizedline[42] + "|" + tokenizedline[43] + "|" + tokenizedline[44] + "|" + tokenizedline[45] + "|" + tokenizedline[46] + "|" + tokenizedline[47] + "|" + tokenizedline[48] + "|" + tokenizedline[49] + "|" + tokenizedline[50] + "|" + tokenizedline[51] + "|" + tokenizedline[52] + "|" + tokenizedline[53] + "|" + tokenizedline[54] + "|" + tokenizedline[55] + "|" + tokenizedline[56] + "|" + tokenizedline[57] + "|" + tokenizedline[58] + "|" + tokenizedline[59] + "|" + tokenizedline[60] + "|" + tokenizedline[61] + "|" + tokenizedline[62] + "|" + tokenizedline[63] + "|" + tokenizedline[64] + "|" + tokenizedline[65] + "|" + tokenizedline[66] + "|" + tokenizedline[67] + "|" + tokenizedline[68] + "|" + tokenizedline[69] + "|" + tokenizedline[70] + "|" + tokenizedline[71] + "|" + tokenizedline[72] + "|" + tokenizedline[73] + "|" + tokenizedline[74] + "|" + tokenizedline[75] + "|" + tokenizedline[76] + "|" + tokenizedline[77] + "|" + tokenizedline[78] + "|" + tokenizedline[79] + "|" + tokenizedline[80] + "|" + tokenizedline[81] + "|" + tokenizedline[82] + "|" + tokenizedline[83] + "|" + tokenizedline[84] + "|" + tokenizedline[85] + "|" + tokenizedline[86] + "|" + tokenizedline[87] + "|" + tokenizedline[88] + "|" + tokenizedline[89] + "|" + tokenizedline[90] + "|" + tokenizedline[91] + "|" + tokenizedline[92] + "|" + tokenizedline[93] + "|" + tokenizedline[94] + "|" + tokenizedline[95] + "|" + tokenizedline[96] + "|" + tokenizedline[97] + "|" + tokenizedline[98] + "|" + tokenizedline[99] + "|" + tokenizedline[100] + "|" + tokenizedline[101] + "|" + tokenizedline[102] + "|" + tokenizedline[103] + "|" + tokenizedline[104] + "|" + tokenizedline[105] + "|" + tokenizedline[106] + "|" + tokenizedline[107] + "|" + tokenizedline[108] + "|" + tokenizedline[109] + "|" + tokenizedline[110] + "|" + tokenizedline[111] + "|" + tokenizedline[112] + "|" + tokenizedline[113] + "|" + tokenizedline[114] + "|" + tokenizedline[115] + "|" + tokenizedline[116] + "|" + tokenizedline[117] + "|" + tokenizedline[118] + "|" + tokenizedline[119] + "|" + tokenizedline[120] + "|" + tokenizedline[121] + "|" + tokenizedline[122] + "|" + tokenizedline[123] + "|" + tokenizedline[124] + "|" + tokenizedline[125] + "|" + tokenizedline[126] + "|" + tokenizedline[127] + "|" + tokenizedline[128] + "|" + tokenizedline[129] + "|" + tokenizedline[130] + "|" + tokenizedline[131] + "|" + tokenizedline[132] + "|" + tokenizedline[133] + "|" + tokenizedline[134] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "|" + tokenizedline[137] + "|" + tokenizedline[138] + "\n").replace("\\","\\\\")
                    ######################
                        
                    # Write the current line to output, formatted for ingest into Postgres.
                    # Pre backslash fix out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + MMSI_string + "\t" + longitude_string + "\t" + latitude_string + "\t" + parsed_date_string + "\t" + str_msg_type + "\t" + str(int(parse_error_flag)) + "\t" + line)
                    out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + MMSI_string + "\t" + longitude_string + "\t" + latitude_string + "\t" + parsed_date_string + "\t" + str_msg_type + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line)

                # Increment the current input line counter.
                in_line_counter += 1
                
# Close the output file.
out_records.close