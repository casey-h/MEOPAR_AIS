#!/usr/bin/python
# Parses out 5 fields from eE formatted AIS data (MMSI, Lat, Lon, Date and Message Type), attaches a Unique ID based  
# on the source file and the row number, an indication as to whether or not there was any problem in parsing the 5 AIS
# fields and, finally, the original CSV row. The output is tab delimited, and any characters with special meaning in
# the Postgres \copy command (i.e. \) are escaped. The data output for the CSV row are restricted to those fields 
# appropriate for the message type, and excludes any fields not cited in the AIS definition for the indicated type. 
# The fields are pipe-delimited rather than the original comma separated, double quote enclosed, format. Rows for 
# which the message type is not available or incorrect are output with all field values. Note that vessel types 
# are expected to be in text eqivalent form as the result of the NMEA parsing scipt (0_gpsd_ais_NM4_parsing.py) used.

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
USAGE_STRING = ("Usage: split_ONC_AIS_msg_type.py datafile_date outputfilename inputfilename \n\n"
"Parses out 5 basic fields (MMSI, Type, Lat, Lon, Date) from Terrestrial AIS data obtained from ONC's "
"online dmas.uvic.ca data service and pre-parsed / formatted, generates a unique line ID. Also tests that the basic fields "
"parse properly. Inserts the 7 generated fields (5 + ID, Flag as result of parse test) along with the original line, all in a "
"tab delimited output file. Uses OV (ONC, Venus) designation along with the datafile date to aid in generating the "
"appropriate unique ID, based on line number within the file. Designed to handle only messages 1,2,3,5,18\n")

# Array of message types with positional information.
POSITIONAL_MESSAGE_TYPES = [1, 2, 3, 18]

# If at least four arguments are not provided, display an usage message.
if (len(sys.argv) < 4):
    print USAGE_STRING
    quit()

# Retrieve the datafile date (as a component of the unique_id to be generated)
datafile_date = sys.argv[1]
    
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

# Retrieve the input filename.
in_filename = sys.argv[3]

# Check the input file for existence before running.
if( not os.path.exists(in_filename)):
    print "Error, input file does not exist: (" + in_filename +  ") aborting."
    quit()
    

# Print a header line for each of the output files to be generated from the eE AIS data.
# Do not write out a header line, gets in the way of \copy - out_records.write("Unq_ID\tMMSI\tLongitude\tLatitude\tDate\tMsgType\tParseError\tAIS_CSV\n")


print("Processing: " + in_filename)

with open(in_filename,'r') as in_vessel_records:

    # Calculate the length of the input filename string.
    in_filename_len = len(in_filename)

    # Calculate a unique ID prefix value based on the input filename. Prepend the source suffix (S or T), plus 'T' 
    # to indicate Taggart/Terrestrial. Also include provided datafile date (presumed to indicate the time span of the
    # incoming datafile e.g yyyy or yyyymm or yyyymmdd). 
    unq_ID_prefix = "OV" + datafile_date + "_"
    
    #CCCCC
    print "unq_ID_prefix: " + unq_ID_prefix 

    # Reset a counter into the input file.
    in_line_counter = 0

    for line in in_vessel_records:

        # Split the input line on pipe characters (output from pre-parsing step)
        tokenizedline = line.strip().split('|')
        
        # Initialize a flag indicating whether or not the base fields from record were found to be parseable.
        parse_error_flag = False

        # Obtain the string containing the message type.
        str_msg_type = tokenizedline[1]

        # Attempt to obtain the message type as an integer from the second token returned by the split operation.
        try:
            input_msg_type = int(str_msg_type)
            
            #If the message type is not in the expected sest (1,2,3,5,18,27), update the value to null and set the parse error flag for the row.
            if(not input_msg_type in (1,2,3,5,18,27)):

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

        # If the message type suggests that longitude and latitude fields should be present, verify that the values are actually coordinates.            
        if(input_msg_type in POSITIONAL_MESSAGE_TYPES):
        
            longitude_string = tokenizedline[8]
            latitude_string = tokenizedline[9]
            
            # If either of the coordinates are not parseable as floating point numbers, check to see if they're just 
            # improperly formatted exponenets (e.g. "1.0E2.0" -- trailing .0 is superfluous and wrong {unless the 
            # system supports fractional powers of 10}, which python doesn't) -- either fix the value, or set it to 
            # null.
            if (not(is_float(longitude_string))): 
            
                suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',longitude_string)
                if(suffix_search is None):

                    print "Longitude parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + longitude_string + ")"
                
                    longitude_string = "\N"
                    parse_error_flag = True
                else:
                    print "Translating: " + longitude_string + " to: " + suffix_search.group(1)
                    longitude_string = suffix_search.group(1)

            if (not(is_float(latitude_string))):
                
                suffix_search = re.search('([-]{0,1}[0-9]+[\.]{0,1}E[+-]{0,1}[0-9]+)(\.[0-9]+)\Z',latitude_string)
                if(suffix_search is None):

                    print "Latitude parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + latitude_string + ")"

                    latitude_string = "\N"
                    parse_error_flag = True
                else:
                    print "Translating: " + latitude_string + " to: " + suffix_search.group(1)
                    latitude_string = suffix_search.group(1)

        # Attempt to set coordinate values for all non-positional message types to \N
        else:

            longitude_string = "\N"
            latitude_string = "\N"      
        
        # If the date value is not of the expected length, or if it is, but has unexpected non-numeric components, set the parse error 
        #flag and insert a null in place of the date.
        raw_date_string = tokenizedline[0]

        # Expected format: YYYYMMDDThhmmss.000Z
        if(len(raw_date_string) != 20):
            print "Date string parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + raw_date_string + ")"
        
            parse_error_flag = True
            parsed_date_string = "\N"
            
        elif (not(is_integer(raw_date_string[0:8])) or not(is_integer(raw_date_string[9:15]))):
            #CCC Debug
            print "Date string parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + raw_date_string + ")"
            
            parse_error_flag = True
            parsed_date_string = "\N"
            
        # If the date is ok, construct a Postgres-acceptable timestamp from the date_string value.
        #e.g 20141001T000005 -> 2014-10-01 00:00:05                 
        else:
            parsed_date_string = raw_date_string[0:4] + "-" + raw_date_string[4:6] + "-" + raw_date_string[6:8] + " " + raw_date_string[9:11] + ":" + raw_date_string[11:13] + ":" + raw_date_string[13:15]

        # If the MMSI is non numeric, set the parse error flag and insert a null in place of the MMSI.
        MMSI_string = tokenizedline[3]
        if(not(is_integer(MMSI_string))):
        
            #CCC Debug
            print "MMSI parse error.(" + unq_ID_prefix + str(in_line_counter) + ": " + MMSI_string + ")"
            
            parse_error_flag = True
            MMSI_string = "\N"
            
        # Output tokenized raw fields according to the message type observed, escape any backslashes in the input line.
        #1_2_3                
        if(input_msg_type in (1, 2, 3)):
             
        
            """Expecting
            MMSI                3
            Message_ID          1
            Repeat_indicator    2
            Time                0
            Millisecond         -
            Region              -
            Country             -
            Base_station        -
            Online_data         -
            Group_code          -
            Sequence_ID         -
            Channel             -
            Data_length         -
            Navigational_status 4
            ROT                 5
            SOG                 6
            Accuracy            7
            Longitude           8
            Latitude            9
            COG                 10
            Heading             11
            Maneuver            13  
            RAIM_flag           14
            Communication_state 15
            UTC_second          12
            spare               -
            """
        
            if(len(tokenizedline) < 16):
        
                print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + line.strip()
                parse_error_flag = True
                PG_safe_line = line.strip().replace("\\","\\\\")
                
                out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                continue
                
            else:
                # Original eE data PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[24] + "|" + tokenizedline[25] + "|" + tokenizedline[26] + "|" + tokenizedline[27] + "|" + tokenizedline[28] + "|" + tokenizedline[29] + "|" + tokenizedline[30] + "|" + tokenizedline[31] + "|" + tokenizedline[33] + "|" + tokenizedline[34] + "|" + tokenizedline[36] + "|" + tokenizedline[42] + "|" + tokenizedline[135] + "\n").replace("\\","\\\\")
                PG_safe_line = (tokenizedline[3] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[0] + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[12] + "|" + "" + "\n").replace("\\","\\\\")

        #5  
        elif(input_msg_type == 5):

            """
            Expecting:
            MMSI                    3
            Message_ID              1
            Repeat_indicator        2
            Time                    0
            Millisecond             -
            Region                  -
            Country                 -
            Base_station            -
            Online_data             -
            Group_code              -
            Sequence_ID             -
            Channel                 -
            Data_length             -
            Vessel_Name             7
            Call_sign               6
            IMO                     5
            Ship_Type               8
            Dimension_to_Bow        9
            Dimension_to_stern      10
            Dimension_to_port       11
            Dimension_to_starboard  12
            Draught                 18
            Destination             19
            AIS_version             4
            Fixing_device           13
            Transmission_control    -
            ETA_month               14
            ETA_day                 15
            ETA_hour                16
            ETA_minute              17
            Sequence                -
            Data_terminal           20
            Mode                    -
            spare                   -
            spare2                  -
            """

            if(len(tokenizedline) < 21):
        
                print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + line.strip()
                parse_error_flag = True
                PG_safe_line = line.strip().replace("\\","\\\\")
                out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                continue
                
            else:
                # Original eE data PG_safe_line = (tokenizedline[0] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[3] + "|" + tokenizedline[4] + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[20] + "|" + tokenizedline[21] + "|" + tokenizedline[22] + "|" + tokenizedline[23] + "|" + tokenizedline[43] + "|" + tokenizedline[44] + "|" + tokenizedline[45] + "|" + tokenizedline[46] + "|" + tokenizedline[47] + "|" + tokenizedline[48] + "|" + tokenizedline[49] + "|" + tokenizedline[65] + "|" + tokenizedline[67] + "|" + tokenizedline[135] + "|" + tokenizedline[136] + "\n").replace("\\","\\\\")
                PG_safe_line = (tokenizedline[3] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[0] + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + tokenizedline[7] + "|" + tokenizedline[6] + "|" + tokenizedline[5] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + tokenizedline[11] + "|" + tokenizedline[12] + "|" + tokenizedline[18] + "|" + tokenizedline[19] + "|" + tokenizedline[4] + "|" + tokenizedline[13] + "|" + "" + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + "" + "|" + tokenizedline[20] + "|" + "" + "|" + "" + "|" + "" + "\n").replace("\\","\\\\")
                
        elif(input_msg_type == 18):

            if(len(tokenizedline) < 21):

                    print "Parse error, invalid number of tokens in input line.\n Line: " + str(in_line_counter) + " - " + line.strip()
                    parse_error_flag = True
                    PG_safe_line = line.strip().replace("\\","\\\\")
                    out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" +  "\N" + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line.strip() + "\n")
                    continue
            else:
                PG_safe_line = (tokenizedline[3] + "|" + tokenizedline[1] + "|" + tokenizedline[2] + "|" + tokenizedline[0] + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + "" + "|" + tokenizedline[5] + "|" + tokenizedline[6] + "|" + tokenizedline[7] + "|" + tokenizedline[8] + "|" + tokenizedline[9] + "|" + tokenizedline[10] + "|" + "" + "|" + tokenizedline[19] + "|"  + "" + "|" + tokenizedline[20] + "|" + "|" + tokenizedline[11] + "" + "|" + "" + "|" + "|" + tokenizedline[18] + "|" + tokenizedline[13] + "|" + tokenizedline[14] + "|" + tokenizedline[15] + "|" + tokenizedline[16] + "|" + tokenizedline[17] + "|" + tokenizedline[4] + "|" + tokenizedline[12] + "\n").replace("\\","\\\\")
            
        else:
            
            print "Parse warning, unhandled message type, skipping \n Line: " + str(in_line_counter) + " - " + line.strip()
            continue
            
        # Write the current line to output, formatted for ingest into Postgres.
        out_records.write(unq_ID_prefix + str(in_line_counter) + "\t" + MMSI_string + "\t" + longitude_string + "\t" + latitude_string + "\t" + parsed_date_string + "\t" + str_msg_type + "\t" + str(int(parse_error_flag)) + "\t" + PG_safe_line)

        # Increment the current input line counter.
        in_line_counter += 1
                
# Close the output file.
out_records.close
