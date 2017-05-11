#!/usr/bin/python
# Split pre-parsed exactEarth position-referenced AIS data (Postgres Export) 
# into basic movement data files on message type group. Requires GDAL / OGR/OSR, and
# progressbar2 packages for Python.

from glob import glob
import sys
import os
import time
import progressbar
from math import radians, cos, sin, asin, sqrt, atan2, pi

# Import the path separator.
from os import sep

# Import OGR/OSR
import osgeo.ogr as ogr
import osgeo.osr as osr

#########################################################
# Constants

# Input AIS data spatial reference (EPSG#) - WGS84
inputEPSG = 4326

# Minimum inferred speed boundary (for point-to-point tracks, GIS file creation) (knots, 2016-04-01)
min_speed_bound_kts = 1

#Maximum inferred speed boundary (for point-to-point tracks, GIS file creation) (knots 2016-04-01)
max_speed_bound_kts = 86.3930885411603
#########################################################

# Function (is_number) to determine if a string represents a number (specifically, an mmsi, code from: http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python).
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
# End function (is_number)


# Function (haversine_coords_km) to calculate approximate distance between point pair in km.
def haversine_coords_km(lon1, lat1, lon2, lat2):

    #Calculate the great circle distance between two points 
    #on the earth (specified in decimal degrees). Adjusted to 
    #return output on the range [0, 360)
    
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth
    km = 6367 * c
    return km 
# End function (haversine_coords)
    
# Function (haversine_geom_m) to calculate approximate distance along segment geometry in m.
def haversine_geom_m(inGeometry):

    #DEBUG
    if (inGeometry.GetPointCount() > 2):
        print "Too many points in haversine?"
    
    # Extract the lat and lon coordinates from the incoming geometry.
    lon1 = inGeometry.GetPoint(0)[0]
    lat1 = inGeometry.GetPoint(0)[1]
    lon2 = inGeometry.GetPoint(1)[0]
    lat2 = inGeometry.GetPoint(1)[1]

    #Calculate the great circle distance between two points 
    #on the earth (specified in decimal degrees)
    
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth / 6367000 metres
    m = 6367000 * c
    return m 
# End function (haversine_geom_m)
    
# Function (bearing) to calculate bearing travelled from one set of coordinates to reach a second.
# Drawn from calculations at: http://www.movable-type.co.uk/scripts/latlong.html
def bearing(lon1, lat1, lon2, lat2):

    # convert decimal degrees to radians 
    lambda_one, phi_one, lambda_two, phi_two = map(radians, [lon1, lat1, lon2, lat2])
    
    # Calculate the difference in x / y movement.
    y = sin(lambda_two-lambda_one) * cos(phi_two);
    x = cos(phi_one)*sin(phi_two) - sin(phi_one)*cos(phi_two)*cos(lambda_two-lambda_one);
        
    # Calculate the initial bearing.
    bearing = atan2(y, x) * 180 / pi
    
    # Convert the bearing to the range [0, 360) for values less than 0.
    if bearing < 0:
        bearing = (bearing + 360) % 360
        
    return bearing
# End function (bearing)

# Function (parseSOG) - Function to interpret possible SOG string values as a numeric value.
def parseSOG(inSOGStr):
    if ((inSOGStr == "n/a") or (inSOGStr == "None") or (inSOGStr == "")):
        return float(102.3)
    elif inSOGStr == "fast":
        return float(102.2)
    else:
        return float(inSOGStr)
# End function (parseSOG)

# Function (parseCOG) - Function to interpret possible COG string values as a numeric value.
def parseCOG(inCOGStr):
    if ((inCOGStr == "n/a") or (inCOGStr == "None") or (inCOGStr == "")):
        return float(360.0)
    else:
        return float(inCOGStr)
# End Function (parseCOG)

# Function (parseTrHdg) - Function to interpret possible True Heading string values as a numeric value.
def parseTrHdg(inTrHdgStr):
    if ((inTrHdgStr == "n/a") or (inTrHdgStr == "None") or (inTrHdgStr == "")):
        return float(511.0)
    else:
        return float(inTrHdgStr)
# End Function (parseTrHdg)

# Function (split_pre_tracks) - Split raw AIS message data into separate files by message type, and then 
# by mmsi.
def split_pre_tracks(in_filename, split_message_file_directory, split_file_directory, split_other_file_directory, out_filename_prefix):
    
    # Define an array of output filenames, based on the provided prefix, to store the parsed results.
    out_filename_array = [split_message_file_directory + sep + out_filename_prefix + "_1_2_3.csv", split_message_file_directory + sep + out_filename_prefix + "_18_19.csv", split_message_file_directory + sep + out_filename_prefix + "_27.csv", split_other_file_directory + sep + out_filename_prefix + "_other.csv"]

    # Check each potential output file for existence before running.
    for outfile_index in range(len(out_filename_array) - 1):
        if os.path.exists(out_filename_array[outfile_index]):
            print "Error, output file exists: (" + out_filename_array[outfile_index] +  ") aborting."
            quit()
            
    # Open all output files required.
    out_message_records = []

    for outfile_index in range(len(out_filename_array)):
        try:
            out_message_records.append(open(out_filename_array[outfile_index], 'w'))
        except IOError:
            print "Error opening output file: " + out_filename_array[outfile_index] + "\n"
            quit()

    # Print a header line for each of the message type groups to be extracted from the eE AIS data.

    #1_2_3 - Type A
    out_message_records[0].write("ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc\n");

    #18_19 - Type B
    out_message_records[1].write("ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc\n");

    #27 - Long range / satellite
    out_message_records[2].write("ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc\n");
        
    #other
    out_message_records[3].write("Field set depends on message type, see split_eE_AIS_for_PG_base_table_w_parsing.py \n")

    print("\nReading input datafile: " + in_filename)

    # Open the input data file and process
    with open(in_filename,'r') as in_vessel_records:

        #Establish a progressbar for iterating over incoming records
        in_records_bar = progressbar.ProgressBar()

        # Reset a counter into the input file.
        in_line_counter = 0

        # Iterate over the incoming records while tracking progress with a progressbar.
        for line in in_records_bar(in_vessel_records):
           
            # Split the input line on the "outer" tab-character based tokenization (generated by Postgres).
            #UNQ_ID\tMMSI\tLON\tLAT\tDATETIME\tMSG_ID\tPARSEERR\tAISMSG
            tabdelline = line.split('\t');
            input_msg_type = tabdelline[5]
            input_longitude = tabdelline[2]
            input_latitude = tabdelline[3]
            
            # Tokenize the record data within the 7th field on the basis of pipe-character.
            pipetokenizedline = tabdelline[7].strip().split('|')
            
            # Output tokenized fields according to the message type observed.
            #1_2_3                
            if(input_msg_type in ("1", "2", "3")):

                # ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc
                out_message_records[0].write(pipetokenizedline[3].strip() + "," + pipetokenizedline[1].strip() + "," + pipetokenizedline[0].strip() + "," + pipetokenizedline[13].strip() + "," + pipetokenizedline[15].strip() + "," + pipetokenizedline[19].strip() + "," + pipetokenizedline[20].strip() + "," + tabdelline[3].strip() + "," + tabdelline[2].strip() + "," + pipetokenizedline[16].strip() + "\n")


            #18_19 
            elif(input_msg_type in ("18", "19")):

                #ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc
                #3,1,0,none,19,23,24,22,21,20
                out_message_records[1].write(pipetokenizedline[3].strip() + "," + pipetokenizedline[1].strip() + "," + pipetokenizedline[0].strip() + "," + "" + "," + pipetokenizedline[19].strip() + "," + pipetokenizedline[23].strip() + "," + pipetokenizedline[24].strip() + "," + tabdelline[3].strip() + "," + tabdelline[2].strip() + "," + pipetokenizedline[20].strip() + "," + "\n")
                
            #27
            elif(input_msg_type in ("27")):
                #ext_timestamp,msgid,mmsi,nav_stat,sog,cog,tr_hdg,lat,lon,pos_acc
                #3,1,0,13,14,18,none,16,17,15
                out_message_records[2].write(pipetokenizedline[3].strip() + "," + pipetokenizedline[1].strip() + "," + pipetokenizedline[0].strip() + "," + pipetokenizedline[13].strip() + "," + pipetokenizedline[14].strip() + "," + pipetokenizedline[18].strip() + "," + "" + "," + tabdelline[3].strip() + "," + tabdelline[2].strip() + "," + pipetokenizedline[15].strip() + "," + "\n")

                
            #other - write out input line as received.
            else:
            
                out_message_records[3].write(line)
                
            # Increment the current input line counter.
            in_line_counter += 1
                        
    # Close / flush all output files required.
    for outfile_index in range(len(out_message_records)):  
        out_message_records[outfile_index].close()
        
    # Run through the 1,2,3; 18,19 and 27 parsed files (all but last in array out_filename_array, 
    # allocating the records within to new output files on the basis of mmsi.
    for data_index in range(len(out_filename_array) - 1):
        
        #Establish a progressbar for iterating over incoming type-divided records
        in_type_records_bar = progressbar.ProgressBar()

        # Reset a counter into the input file.
        in_line_counter = 0

        # Display a message about the stage of processing
        print("\nReading type-split datafile: " + os.path.basename(out_filename_array[(data_index)]))
        
        with open(out_filename_array[data_index],'r') as in_parsed_AIS_underway:
        
            # Iterate over the incoming records while tracking progress with a progressbar.
            for line in in_type_records_bar(in_parsed_AIS_underway):
                
                tokenizedline = line.split(',')

                # Verify that the incoming line has the correct number of tokens.
                if(len(tokenizedline) > 8):
                    
                    # Note: because the parser for NM4 data returns n/a for missing mmsi
                    # (unlike the original eE dataset, which uses 0), added a catch here
                    # to replace the / values with _ to generate valid filenames. 
                    mmsi = tokenizedline[2].replace('/','_')
                    
                    # Verify that the mmsi extracted is a valid number, then write the results into an output file.
                    if is_number(mmsi):
                    
                        try:
                            outfile = open(split_file_directory  + sep + mmsi + ".txt", 'a')
                        except IOError:
                            print "Error opening file: " + split_file_directory + sep + mmsi + "\n"
                            quit()
                            
                    #If the "mmsi" is non numeric, write it to the "other" directory.
                    else:
                        try:
                            outfile = open(split_other_file_directory  + sep + mmsi + ".txt", 'a')
                        except IOError:
                            print "Error opening file: " + split_other_file_directory + sep + mmsi + "\n"
                            quit()
                        
                    outfile.write(line)
                    outfile.close()
                else:
                    
                    print("Error, incorrect number of tokens:" + str(len(tokenizedline)) + "Line:" + line + "\n")
# End Function (split_pre_tracks)

# Function (generate_short_segments) - Split mmsi-separated text position data into short segments
# based on identified thresholds.
def generate_short_segments(split_file_directory, segment_split_directory, segment_other_split_directory, max_elapsed_time):
    
    #Establish a progressbar for iterating over incoming split files
    in_split_files_bar = progressbar.ProgressBar()

    # Reset a counter into the input file.
    in_line_counter = 0

    # Iterate over the split files generated in the previous step as the data are inserted into the output layer.
    for split_filename in in_split_files_bar(glob(split_file_directory  + sep + "*.txt")):
    
        (dummyone, dummytwo, outfilename) = split_filename.rpartition(sep)

        try:
            out_vessel_records = open(segment_split_directory + sep + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_split_directory + sep + outfilename + "\n"
            quit()

        try:
            out_stationary_records = open(segment_other_split_directory + sep + "stationary_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_other_split_directory + sep + "stationary_" + outfilename + "\n"
            quit()
            
        try:
            out_invalid_records = open(segment_other_split_directory + sep + "invalid_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_other_split_directory + sep + "invalid_" + outfilename + "\n"
            quit()

        with open(split_filename,'r') as in_vessel_records:
            
            # Initialize defaults.
            track_index = 0
            prev_timeval = 0
            prev_latitude = -999
            prev_longitude = -999
            
            track_point_counter = 0
            
            # Tokenize each line of input from the source file, presuming comma delimiters.
            for line in in_vessel_records:
                
                tokenizedline = line.split(',')
                datetimetoken = tokenizedline[0]
                msgid = tokenizedline[1]
                mmsi = tokenizedline[2]
                navstatus = tokenizedline[3]
                sog = tokenizedline[4]
                cog = tokenizedline[5]
                tr_hdg = tokenizedline[6]
                latitude = tokenizedline[7]
                longitude = tokenizedline[8]
                pos_acc = tokenizedline[9]
                
                # If the time token contains an underscore, assume the time format "%Y%m%d_%H%M%S", 
                # else if it contains a T, assume the time format "%Y%m%dT%H%M%S.000Z", otherwise 
                # abort for unrecognized time format.
                if (datetimetoken.find("_") > -1):
                    timevalstruct = time.strptime(datetimetoken, "%Y%m%d_%H%M%S")
                elif (datetimetoken.find("T") > -1):
                    #timevalstruct = time.strptime(datetimetoken, "%Y%m%dT%H%M%S.000Z")
                    # Added %f in an attempt to handle milliseconds for T-AIS (http://stackoverflow.com/questions/698223/how-can-i-parse-a-time-string-containing-milliseconds-in-it-with-python)
                    timevalstruct = time.strptime(datetimetoken, "%Y%m%dT%H%M%S.%fZ")
                else:
                    print "Unrecognized date/time format, aborting: " + datetimetoken + "\n"
                    out_vessel_records.close()
                    out_stationary_records.close()
                    out_invalid_records.close()
                    quit()
            
                timeval = time.mktime(timevalstruct)

                # If the vessel's position is outside the reasonable range (e.g. 91 indicating unavailable within eE AIS, or n/a 
                # from AIS parsed using GPSD parser.), 
                # drop the position into the file for invalid records.
                if latitude == 'n/a' or ((abs(float(latitude)) >= 90)):
                    out_invalid_records.write(line)
                        
                # If the vessel's position has not changed between records, drop the position 
                # into the file for stationary records (Note: Could / should be replaced with 
                # subroutine to seek and drop all strings of stationary positions as discrete 
                # stationary objects).
                elif ((latitude == prev_latitude) and (longitude == prev_longitude)) or (prev_timeval == timeval):
                
                    out_stationary_records.write(line)
                    
                else:
                    
                    # If the current point is the first of a track segment, store the value 
                    # and wait until further points accrue before writing out values.
                    if(track_point_counter == 0):
                        
                        # Increment the track point counter.
                        track_point_counter = 1
                        
                        # Store the current values as previous ("-1").
                        prev_datetimetoken = datetimetoken
                        prev_msgid = msgid
                        prev_mmsi = mmsi
                        prev_navstatus = navstatus
                        prev_sog = sog
                        prev_cog = cog
                        prev_tr_hdg = tr_hdg
                        prev_latitude = latitude
                        prev_longitude = longitude
                        prev_pos_acc = pos_acc
                        prev_timeval = timeval
                    
                    # If the current point is the second of a track segment, calculate the 
                    # speed and direction, and write out the pair to the output.
                    elif(track_point_counter == 1):
                        
                        # Calculate the elapsed time between the points.
                        elapsed_segment_time = timeval - prev_timeval
                        
                        # Calculate the distance between the positions.
                        segment_length = haversine_coords_km(float(prev_longitude), float(prev_latitude), float(longitude), float(latitude))
                        
                        # Calculate the bearing between the positions.
                        segment_bearing = bearing(float(prev_longitude), float(prev_latitude), float(longitude), float(latitude))
                        
                        # CCCC - Removed navstatus from output, was unused; could revisit later.
                        
                        # Print out the first point of the pair.
                        # old 20150915 out_vessel_records.write("" + str(track_index) + "," + datetimetoken + "," + mmsi + "," + str(latitude) + "," + str(longitude) + "," + shiptype + "," + shipname + "," + str(elapsed_segment_time) + "," + str(segment_length) + "," + str(segment_bearing))
                        out_vessel_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + str(prev_latitude) + "," + str(prev_longitude) + "," + prev_pos_acc)
                        
                        # Print out the second point of the pair
                        # old 20150915 out_vessel_records.write("" + str(track_index) + "," + prev_datetimetoken + "," + prev_mmsi + "," + str(prev_latitude) + "," + str(prev_longitude) + "," + prev_shiptype + "," + prev_shipname + "," + str(elapsed_segment_time) + "," + str(segment_length) + "," + str(segment_bearing))                        
                        out_vessel_records.write("" + str(track_index) + "," + str(int(timeval)) + "," + datetimetoken + "," + msgid + "," + mmsi + "," + navstatus + "," + sog + "," + cog + "," + tr_hdg + "," + str(latitude) + "," + str(longitude) + "," + pos_acc)

                        # Leave the track point counter at 1.
                        track_point_counter = 1
                        
                        # Increment the track index.
                        track_index += 1
                        
                        # Store the current values as previous.
                        prev_datetimetoken = datetimetoken
                        prev_msgid = msgid
                        prev_mmsi = mmsi
                        prev_navstatus = navstatus
                        prev_sog = sog
                        prev_cog = cog
                        prev_tr_hdg = tr_hdg
                        prev_latitude = latitude
                        prev_longitude = longitude
                        prev_pos_acc = pos_acc
                        prev_timeval = timeval
     
        # Close the output files.
        out_vessel_records.close()
        out_stationary_records.close()
        out_invalid_records.close()
    
def generate_short_GIS(segment_split_directory, gis_directory, out_filename_prefix, max_elapsed_time, outputEPSG):
    
    # Generate a line file output filename.
    out_line_filename = os.path.basename(gis_directory + sep + out_filename_prefix + "_lines")
    out_line_directory = os.path.dirname(gis_directory + sep + out_filename_prefix + "_lines")

    # Set up the shapefile driver.
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Create the data source.
    track_data_source = driver.CreateDataSource(out_line_directory)

    # create the spatial reference, WGS84
    in_srs = osr.SpatialReference()
    in_srs.ImportFromEPSG(inputEPSG)

    # Create the output spatial reference, BC Albers
    # Presuming effective equality between NAD83 / WGS84 over function scope (e.g. North America -- https://www.packtpub.com/books/content/working-geo-spatial-data-python)
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(outputEPSG)

    # Create a transform object to project WGS84 coordinates to user-specified output.
    transform = osr.CoordinateTransformation(in_srs, out_srs)

    # Create the track layer.
    track_layer = track_data_source.CreateLayer(out_line_filename, out_srs, ogr.wkbLineString)
    if track_layer is None:
        print "\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting."
        quit()

    # Define the data fields to be included in the output track layer.
    track_layer.CreateField(ogr.FieldDefn("TrackID", ogr.OFTInteger))
    track_field_mmsi = ogr.FieldDefn("mmsi", ogr.OFTString)
    track_field_mmsi.SetWidth(12)
    track_layer.CreateField(track_field_mmsi)
    track_layer.CreateField(ogr.FieldDefn("elp_sec", ogr.OFTInteger))
    track_field_st_date = ogr.FieldDefn("st_date", ogr.OFTString)
    track_field_st_date.SetWidth(20)
    track_layer.CreateField(track_field_st_date)
    track_field_en_date = ogr.FieldDefn("en_date", ogr.OFTString)
    track_field_en_date.SetWidth(20)
    track_layer.CreateField(track_field_en_date)
    line_field_speed = ogr.FieldDefn("speed_kts", ogr.OFTReal)
    line_field_speed.SetWidth(5)
    line_field_speed.SetPrecision(1)
    track_layer.CreateField(line_field_speed)
    line_field_len = ogr.FieldDefn("seg_len_km", ogr.OFTReal)
    line_field_len.SetWidth(12)
    line_field_len.SetPrecision(6)
    track_layer.CreateField(line_field_len)

    # Initialize counters of the number of discarded and preserved segments.
    preserved_count = 0
    speed_discarded_count_fast = 0
    speed_discarded_count_slow = 0
    time_discarded_count = 0
    
    # Print a message about the stage of processing
    print "\nGenerating short (point-to-point) GIS layer generation."
    
    #Establish a progressbar for iterating over incoming split files
    in_segment_split_files_bar = progressbar.ProgressBar()

    # Reset a counter into the input file.
    in_line_counter = 0

    # Iterate over the divided files generated in the previous step as the data are inserted into the output layer.
    for segment_split_filename in in_segment_split_files_bar(glob(segment_split_directory  + sep + "*.txt")):

        with open(segment_split_filename,'r') as in_track_records:
        
            # Initialize an index into the track ID values and a list to hold waypoints while tracks are constructed.
            prev_inTrackID = -1

            track_wkt = 'LINESTRING ('
            track_len = 0
            
            # Iterate over the lines in the current file of track data.
            for line in in_track_records:
                
                # Split the incoming csv line.
                tokenizedline = line.split(',')
                inTrackID = int(tokenizedline[0])

                # If the current line is the first of the input file, initialize variables.
                if(prev_inTrackID == -1):
                    
                    prev_inTrackID = inTrackID
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMinSeconds = int(tokenizedline[1])
                    inMaxSeconds = inMinSeconds
                    inPrevSeconds = inMinSeconds
                    inMinDate = tokenizedline[2]
                    inMaxDate = inMinDate
                    inMMSI = tokenizedline[4]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Initialize a flag to store whether or not questionable implided speeds were encountered for a given track.
                    badSpeedFlag = 0 
                    
                    # Append the current point to the list of points for the track.
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    

                # If the current line is the first of a new track, terminate the existing track 
                # before initializing the next.
                elif(prev_inTrackID <> inTrackID):
                    
                    # If the existing track has at least two points, proceed with writing it out.
                    if(track_len > 1):
                    
                        # Proceed only if the points are not coincident in time and the elapsed 
                        # time is less than the threshold.
                        if (inMaxSeconds <> inMinSeconds) and (inMaxSeconds - inMinSeconds < max_elapsed_time):
                            
                            # Close the track geometry and create a feature object.
                            track_wkt = track_wkt + ")"
                            track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                            
                            # Project the feature object to target from WGS84 unprojected 
                            # (assuming datum shift is inconsequential {NA}.)
                            track_obj.Transform(transform)
                            
                            # If the segment is made of projected points, calculate its length under projection
                            # Calculate the length of the segment under projection
                            # speed in knots  (https://en.wikipedia.org/wiki/Knot_(unit))
                            if (outputEPSG <> 4326):
                                track_len_m = track_obj.Length()
                                speed_knots = (track_len_m / float(1852)) / ((inMaxSeconds - inMinSeconds) / float(3600))
                            # If the segment is made of unprojected points, calculate its length using the haversine
                            # formula (approximation of length), and corresponding speed in knots.
                            else:
                                track_len_m = haversine_geom_m(track_obj)
                                speed_knots = (track_len_m / float(1852)) / ((inMaxSeconds - inMinSeconds) / float(3600))
                            
                            
                            # If the segment is within the valid range of speeds for consideration, 
                            # write it to the output, otherwise, discard it. (86.3930885411603 knots = 160kph)
                            if (speed_knots > min_speed_bound_kts):
                                if (speed_knots < max_speed_bound_kts):
                                
                                    # Generate a feature and and populate its fields.
                                    track_feature = ogr.Feature(track_layer.GetLayerDefn())
                                    track_feature.SetField("TrackID" ,prev_inTrackID)
                                    track_feature.SetField("mmsi" ,inMMSI)
                                    track_feature.SetField("elp_sec" ,(inMaxSeconds - inMinSeconds))
                                    track_feature.SetField("st_date" , inMinDate)
                                    track_feature.SetField("en_date" , inMaxDate)
                                    track_feature.SetField("seg_len_km" , track_len_m / float(1000))
                                    track_feature.SetField("speed_kts" , speed_knots)
                                    
                                    # Assign the geometry to the feature.
                                    track_feature.SetGeometry(track_obj)
                                    
                                    # Create the feature within the output layer, then reclaim assigned memory.
                                    track_layer.CreateFeature(track_feature)
                                    track_feature.Destroy()
                                    
                                    preserved_count = preserved_count + 1
                                    
                                else:
                                    speed_discarded_count_fast = speed_discarded_count_fast + 1
                            else:
                                speed_discarded_count_slow = speed_discarded_count_slow + 1
                                
                        else:
                            time_discarded_count = time_discarded_count + 1
                            
                    
                    prev_inTrackID = inTrackID
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMinSeconds = int(tokenizedline[1])
                    inMaxSeconds = inMinSeconds
                    inPrevSeconds = inMinSeconds
                    inMinDate = tokenizedline[2]
                    inMaxDate = inMinDate
                    inMMSI = tokenizedline[4]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Reset the existing list of points / append the current point to the list of points for the next track.
                    track_wkt = 'LINESTRING (' + str(inLon) + " " + str(inLat)
                    track_len = 1

                     ### (End) Add the current point to the point output layer
                    
                # If the current line extends the current track, aggregate the values as required.
                else:
                    
                    # Copy the previous timestamp and position values.
                    inPrevSeconds = inMaxSeconds
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMaxSeconds = int(tokenizedline[1])
                    inMaxDate = tokenizedline[2]
                    inMMSI = tokenizedline[4]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])                     
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    
                    # If the distance and time between the previous and current position indicates a speed of 
                    # greater than 160 kph (0.044444 km per s), set a 'bad speed' flag on the record.
                        
                    # Append the current point to the list of points for the track.
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    
            # If the last remaining track has at least two points, proceed with writing it out.
            if(track_len > 1):
                
                # Proceed only if the points are not coincident in time and the elapsed 
                # time is less than the threshold.
                if (inMaxSeconds <> inMinSeconds) and (inMaxSeconds - inMinSeconds < max_elapsed_time):
                
                    # Close the track geometry and create a feature object.
                    track_wkt = track_wkt + ")"
                    track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                    
                    # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                    # (assuming datum shift is inconsequential {NA}.)
                    track_obj.Transform(transform)
                    
                    # If the segment is made of projected points, calculate its length under projection
                    # Calculate the length of the segment under projection
                    # speed in knots  (https://en.wikipedia.org/wiki/Knot_(unit))
                    if (outputEPSG <> 4326):
                        track_len_m = track_obj.Length()
                        speed_knots = (track_len_m / float(1852)) / ((inMaxSeconds - inMinSeconds) / float(3600))
                    # If the segment is made of unprojected points, calculate its length using the haversine
                    # formula (approximation of length), and corresponding speed in knots.
                    else:
                        track_len_m = haversine_geom_m(track_obj)
                        speed_knots = (track_len_m / float(1852)) / ((inMaxSeconds - inMinSeconds) / float(3600))
                    
                    # If the segment is within the valid range of speeds for consideration, 
                    # write it to the output, otherwise, discard it.
                    if (speed_knots > min_speed_bound_kts):
                        if (speed_knots < max_speed_bound_kts):
                        
                            # Generate a feature and and populate its fields.
                            track_feature = ogr.Feature(track_layer.GetLayerDefn())
                            track_feature.SetField("trackid" ,prev_inTrackID)
                            track_feature.SetField("mmsi" ,inMMSI)
                            track_feature.SetField("elp_sec" ,(inMaxSeconds - inMinSeconds))
                            track_feature.SetField("st_date" , inMinDate)
                            track_feature.SetField("en_date" , inMaxDate)
                            track_feature.SetField("seg_len_km" , track_len_m / float(1000))
                            track_feature.SetField("speed_kts" , speed_knots)

                            # Assign the geometry to the feature.
                            track_feature.SetGeometry(track_obj)
                            
                            # Create the feature within the output layer, then reclaim assigned memory.
                            track_layer.CreateFeature(track_feature)
                            track_feature.Destroy()
                            
                            preserved_count = preserved_count + 1
                            
                        else:
                                
                            speed_discarded_count_fast = speed_discarded_count_fast + 1
                    else:
                    
                        speed_discarded_count_slow = speed_discarded_count_slow + 1                        
                        
                else:
                
                    time_discarded_count = time_discarded_count + 1

    # Destroy the data source to flush features to disk.
    track_data_source.Destroy()

    # Print out the number of preserved and discarded segments.
    print "\nCompleted, " + str(preserved_count) + " segments generated, " + str(speed_discarded_count_slow) + " discarded for invalid speed (slow), " + str(speed_discarded_count_fast) + " discarded for invalid speed (fast), " + str(time_discarded_count) + " discarded for invalid time.\n"
    # Print out the thresholds applied in processing.
    print "Speed bounds: " + str(min_speed_bound_kts) + " < speed in knots < " + str(max_speed_bound_kts)
    print "Segment (temporal) length bounds 0 < time in seconds < " + str(max_elapsed_time) + "\n"

def generate_threshold_tracks(split_file_directory, segment_split_directory, segment_other_split_directory, track_separation_time, max_point_speed):
    
    #Establish a progressbar for iterating over incoming split files
    in_split_files_bar = progressbar.ProgressBar()
    
    # Convert the max speed value in kph to kilometres per second to match with script.
    track_speed_threshold = max_point_speed / float(3600)

    # Iterate over the split files generated in the previous step.
    for split_filename in in_split_files_bar(glob(split_file_directory  + sep + "*.txt")):
        
        #print("Processing: " + split_filename)

        (dummyone, dummytwo, outfilename) = split_filename.rpartition(sep)
        
        try:
            out_vessel_records = open(segment_split_directory + sep + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_split_directory + sep + outfilename + "\n"
            quit()

        try:
            out_stationary_records = open(segment_other_split_directory + sep + "stationary_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_other_split_directory + sep + "stationary_" + outfilename + "\n"
            quit()
            
        try:
            out_invalid_records = open(segment_other_split_directory + sep + "invalid_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_other_split_directory + sep + "invalid_" + outfilename + "\n"
            quit()
            
        try:
            out_orphaned_records = open(segment_other_split_directory + sep + "orphaned_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + segment_other_split_directory + sep + "orphaned_" + outfilename + "\n"
            quit()

        with open(split_filename,'r') as in_vessel_records:
            
            # Initialize defaults.
            track_index = 0
            prev_timeval = 0
            prev_latitude = -999
            prev_longitude = -999
            
            track_point_counter = 0
            
            # Tokenize each line of input from the source file, presuming comma delimiters.
            for line in in_vessel_records:
                
                tokenizedline = line.split(',')
                datetimetoken = tokenizedline[0]
                msgid = tokenizedline[1]
                mmsi = tokenizedline[2]
                navstatus = tokenizedline[3]
                sog = tokenizedline[4]
                cog = tokenizedline[5]
                tr_hdg = tokenizedline[6]
                latitude = tokenizedline[7]
                longitude = tokenizedline[8]
                pos_acc = tokenizedline[9]
                
                timevalstruct = time.strptime(datetimetoken, "%Y%m%d_%H%M%S")
                timeval = time.mktime(timevalstruct)
                
                # If the vessel's position is outside the reasonable range (e.g. 91 indicating unavailable within eE AIS, or n/a 
                # from AIS parsed using GPSD parser.), 
                # drop the position into the file for invalid records.
                if latitude == 'n/a' or ((abs(float(latitude)) >= 90)):
                    out_invalid_records.write(line)
                        
                # If the vessel's position has not changed between records, drop the position 
                # into the file for stationary records (Note: Could / should be replaced with 
                # subroutine to seek and drop all strings of stationary positions as discrete 
                # stationary objects).
                elif ((latitude == prev_latitude) and (longitude == prev_longitude)) or (prev_timeval == timeval):
                    out_stationary_records.write(line)
                    
                else:
                    
                    # If the current point is the first of a track segment, store the value 
                    # and wait until further points accrue before writing out values.
                    if(track_point_counter == 0):
                        
                        # Increment the track point counter.
                        track_point_counter = 1
                        
                        # Store the current values as previous ("-1").
                        prev_datetimetoken = datetimetoken
                        prev_msgid = msgid
                        prev_mmsi = mmsi
                        prev_navstatus = navstatus
                        prev_sog = sog
                        prev_cog = cog
                        prev_tr_hdg = tr_hdg
                        prev_latitude = latitude
                        prev_longitude = longitude
                        prev_pos_acc = pos_acc
                        prev_timeval = timeval
                        
                    # If the current point is the second of a track segment, store the 
                    # value and wait until further points accrue before writing out
                    # values.
                    elif(track_point_counter == 1):
                        
                        # If the time interval between two successive points is less than the time interval 
                        # suggesting track separation (180s), and there is no change in navigation status, 
                        # proceed with storing the current point for further processing.
                        if (timeval - prev_timeval < track_separation_time) and (navstatus == prev_navstatus):
                        
                            # Increment the track point counter.
                            track_point_counter = 2

                            # Store the previous values as two steps back ("-2").
                            prev_prev_datetimetoken = prev_datetimetoken
                            prev_prev_msgid = prev_msgid
                            prev_prev_mmsi = prev_mmsi
                            prev_prev_navstatus = prev_navstatus
                            prev_prev_sog = prev_sog
                            prev_prev_cog = prev_cog
                            prev_prev_tr_hdg = prev_tr_hdg
                            prev_prev_latitude = prev_latitude
                            prev_prev_longitude = prev_longitude
                            prev_prev_pos_acc = prev_pos_acc
                            prev_prev_timeval = prev_timeval
                            
                            # Store the current values as previous ("-1").
                            prev_datetimetoken = datetimetoken
                            prev_msgid = msgid
                            prev_mmsi = mmsi
                            prev_navstatus = navstatus
                            prev_sog = sog
                            prev_cog = cog
                            prev_tr_hdg = tr_hdg
                            prev_latitude = latitude
                            prev_longitude = longitude
                            prev_pos_acc = pos_acc
                            prev_timeval = timeval
                            
                        # If the time interval between points is greater than or equal to the time interval
                        # suggesting track separation, or the navigation status has changed, store the current 
                        # values as previous, and set the track point counter as 1.
                        else:
                            
### (Begin) This condition suggests an orphaned point (the existing -1/"prev_" point)
                            out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
### (End) This condition suggests an orphaned point (the existing -1/"prev_" point)
                            
                            # Set the track point counter as 1.
                            track_point_counter = 1
                            
                            # Store the current values as previous ("-1").
                            prev_datetimetoken = datetimetoken
                            prev_msgid = msgid
                            prev_mmsi = mmsi
                            prev_navstatus = navstatus
                            prev_sog = sog
                            prev_cog = cog
                            prev_tr_hdg = tr_hdg
                            prev_latitude = latitude
                            prev_longitude = longitude
                            prev_pos_acc = pos_acc
                            prev_timeval = timeval
                        
                    else:
                        
                        # If the navstatus has changed between successive points, or the time interval 
                        # suggests track separation (180s), divide the track at that point, and tidy up 
                        # any remaining track points.
                        if(prev_navstatus <> prev_prev_navstatus) or (prev_timeval - prev_prev_timeval >= track_separation_time):

                            # If there have been at least 3 points considered, include the current position
                            # only if the speed value calculated in the previous iteration suggested that it 
                            # was a valid part of the track.
                            if (track_point_counter > 2) and (spd_one < track_speed_threshold):
                                out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
                                
                            track_index += 1
                            
                            # Set the track point counter at 2 points.
                            track_point_counter = 2
                            
                            # Store the previous values as two steps back ("-2").
                            prev_prev_datetimetoken = prev_datetimetoken
                            prev_prev_msgid = prev_msgid
                            prev_prev_mmsi = prev_mmsi
                            prev_prev_navstatus = prev_navstatus
                            prev_prev_sog = prev_sog
                            prev_prev_cog = prev_cog
                            prev_prev_tr_hdg = prev_tr_hdg
                            prev_prev_latitude = prev_latitude
                            prev_prev_longitude = prev_longitude
                            prev_prev_pos_acc = prev_pos_acc
                            prev_prev_timeval = prev_timeval
                            
                            # Store the current values as previous ("-1").
                            prev_datetimetoken = datetimetoken
                            prev_msgid = msgid
                            prev_mmsi = mmsi
                            prev_navstatus = navstatus
                            prev_sog = sog
                            prev_cog = cog
                            prev_tr_hdg = tr_hdg
                            prev_latitude = latitude
                            prev_longitude = longitude
                            prev_pos_acc = pos_acc
                            prev_timeval = timeval
                        
                        # If the time interval between two successive points is less than the time interval 
                        # suggesting track separation (180s), proceed with validating the inter-waypoint speed.
                        #elif (prev_timeval - prev_prev_timeval < track_separation_time):
                        else:
                            
                            # Calculate the implied speeds between the -2 and -1 positions and the -2 and 0 positions. Add a 
                            # check for 0 elapsed distance or time.
                            dist_one = haversine_coords_km(float(prev_prev_longitude), float(prev_prev_latitude), float(prev_longitude), float(prev_latitude))
                            if (dist_one == 0):
                                spd_one = 0
                            else:
                                ela_time_one = float(prev_timeval) - float(prev_prev_timeval)
                                if(ela_time_one > 0):
                                    spd_one = dist_one / ela_time_one
                                else:
                                    spd_one = 999
                            
                            dist_two = haversine_coords_km(float(prev_prev_longitude), float(prev_prev_latitude), float(longitude), float(latitude))
                            if(dist_two == 0):
                                spd_two = 0
                            else:
                                ela_time_two = float(timeval) - float(prev_prev_timeval)
                                if(ela_time_two > 0):
                                    spd_two = dist_two / ela_time_two
                                else:
                                    spd_two = 999
                        
                            # If the calculated speed is below 0.5 knots, presume the vessel has effectively stopped, and divide the track.
# CH Note: This should probably be checked as well at the leading and terminating point pairs
# CH - Move this up, specify as arg.
                            if(spd_one < 0.000257222222): 
                            
                                # If there have been at least 3 points considered, include the current position
                                # only if the speed value calculated in the previous iteration suggested that it 
                                # was a valid part of the track.
                                if (track_point_counter > 2) and (spd_one < track_speed_threshold):
                                    
                                    out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
                                
                                # Increment the track point counter.
                                track_index += 1
                                
                                # Set the track point counter at 2 points.
                                track_point_counter = 2

                                # Store the previous values as two steps back ("-2").
                                prev_prev_datetimetoken = prev_datetimetoken
                                prev_prev_msgid = prev_msgid
                                prev_prev_mmsi = prev_mmsi
                                prev_prev_navstatus = prev_navstatus
                                prev_prev_sog = prev_sog
                                prev_prev_cog = prev_cog
                                prev_prev_tr_hdg = prev_tr_hdg
                                prev_prev_latitude = prev_latitude
                                prev_prev_longitude = prev_longitude
                                prev_prev_pos_acc = prev_pos_acc
                                prev_prev_timeval = prev_timeval
                                
                                # Store the current values as previous ("-1").
                                prev_datetimetoken = datetimetoken
                                prev_msgid = msgid
                                prev_mmsi = mmsi
                                prev_navstatus = navstatus
                                prev_sog = sog
                                prev_cog = cog
                                prev_tr_hdg = tr_hdg
                                prev_latitude = latitude
                                prev_longitude = longitude
                                prev_pos_acc = pos_acc
                                prev_timeval = timeval
                                
                            # If the speed is at least 0.5 knots, continue processing.
                            else:
                        
                                # If the speed implied between the -2 and -1 positions is below the upper
                                # reasonable threshold (160kph), write out the -2 position data and update
                                # the previous values.
                                if(spd_one < track_speed_threshold):
                                
                                    out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
                                
                                    # Increment the track point counter.
                                    track_point_counter = track_point_counter + 1
                                        
                                    # Store the previous values as two steps back ("-2").
                                    prev_prev_datetimetoken = prev_datetimetoken
                                    prev_prev_msgid = prev_msgid
                                    prev_prev_mmsi = prev_mmsi
                                    prev_prev_navstatus = prev_navstatus
                                    prev_prev_sog = prev_sog
                                    prev_prev_cog = prev_cog
                                    prev_prev_tr_hdg = prev_tr_hdg
                                    prev_prev_latitude = prev_latitude
                                    prev_prev_longitude = prev_longitude
                                    prev_prev_pos_acc = prev_pos_acc
                                    prev_prev_timeval = prev_timeval
                                    
                                    # Store the current values as previous ("-1").
                                    prev_datetimetoken = datetimetoken
                                    prev_msgid = msgid
                                    prev_mmsi = mmsi
                                    prev_navstatus = navstatus
                                    prev_sog = sog
                                    prev_cog = cog
                                    prev_tr_hdg = tr_hdg
                                    prev_latitude = latitude
                                    prev_longitude = longitude
                                    prev_pos_acc = pos_acc
                                    prev_timeval = timeval
                                
                                # If the speed implied between the -2 and -1 positions is above the upper
                                # reasonable threshold and the -2 and 0 positions are also above the
                                # threshold, assume that the -2 position point is invalid, and replace it
                                # with the -1 point before proceeding to the next iteration to eliminate 
                                # the invalid point.
                                elif(spd_two > track_speed_threshold):

### (Begin) This condition suggests an orphaned point (the existing -2/"prev_prev_" point)
                                    out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
### (End) This condition suggests an orphaned point (the existing -2/"prev_prev_" point)

                                    # Store the previous values as two steps back ("-2").
                                    prev_prev_datetimetoken = prev_datetimetoken
                                    prev_prev_msgid = prev_msgid
                                    prev_prev_mmsi = prev_mmsi
                                    prev_prev_navstatus = prev_navstatus
                                    prev_prev_sog = prev_sog
                                    prev_prev_cog = prev_cog
                                    prev_prev_tr_hdg = prev_tr_hdg
                                    prev_prev_latitude = prev_latitude
                                    prev_prev_longitude = prev_longitude
                                    prev_prev_pos_acc = prev_pos_acc
                                    prev_prev_timeval = prev_timeval
                                    
                                    # Store the current values as previous ("-1").
                                    prev_datetimetoken = datetimetoken
                                    prev_msgid = msgid
                                    prev_mmsi = mmsi
                                    prev_navstatus = navstatus
                                    prev_sog = sog
                                    prev_cog = cog
                                    prev_tr_hdg = tr_hdg
                                    prev_latitude = latitude
                                    prev_longitude = longitude
                                    prev_pos_acc = pos_acc
                                    prev_timeval = timeval
                                    
                                    
                                # If the speed implied between the -2 and -1 positions is above the upper
                                # reasonable threshold and the -2 and 0 positions is not, assume that the 
                                # -1 position point is invalid and skip to the next iteration to eliminate 
                                # the invalid point.
                                else:

### (Begin) This condition suggests an orphaned point (the existing -1/"prev_" point)
                                    out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
### (End) This condition suggests an orphaned point (the existing -1/"prev_" point)

                                    # Store the current values as previous ("-1").
                                    prev_datetimetoken = datetimetoken
                                    prev_msgid = msgid
                                    prev_mmsi = mmsi
                                    prev_navstatus = navstatus
                                    prev_sog = sog
                                    prev_cog = cog
                                    prev_tr_hdg = tr_hdg
                                    prev_latitude = latitude
                                    prev_longitude = longitude
                                    prev_pos_acc = pos_acc
                                    prev_timeval = timeval

            # Clean up any remaining points in the -2 and -1 positions.
            if (track_point_counter == 2):
                
                # If there is no significant time gap between the -2 and -1 points, consider both for processing
                if (prev_timeval - prev_prev_timeval < track_separation_time):
                    
                    # Calculate the implied speeds between the -2 and -1 positions.
                    prev_prev_dist = haversine_coords_km(float(prev_prev_longitude), float(prev_prev_latitude), float(prev_longitude), float(prev_latitude))
                    prev_prev_spd = prev_prev_dist / (float(prev_prev_timeval) - float(prev_timeval))
                    
                    # If the speed implied between the -2 and -1 positions is below the upper
                    # reasonable threshold (160kph), write out the -2 and -1 position data.
                    if(prev_prev_spd < track_speed_threshold):
                   
                        out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
                        out_vessel_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
     
                    else:
### (Begin) This condition suggests an orphaned point (the existing -1/"prev_" point)
                        out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
### (End) This condition suggests an orphaned point (the existing -1/"prev_" point)

                        out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)
                        
                # If there is a significant time gap between the -2 and -1 points, output only the -2 position.
                else:
                
### (Begin) This condition suggests an orphaned point (the existing -1/"prev_" point)
                        out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
### (End) This condition suggests an orphaned point (the existing -1/"prev_" point)

                        out_vessel_records.write("" + str(track_index) + "," + str(int(prev_prev_timeval)) + "," + prev_prev_datetimetoken + "," + prev_prev_msgid + "," + prev_prev_mmsi + "," + prev_prev_navstatus + "," + prev_prev_sog + "," + prev_prev_cog + "," + prev_prev_tr_hdg + "," + prev_prev_latitude + "," + prev_prev_longitude + "," + prev_prev_pos_acc)

            # If track_point_counter == 1, suggests a dropped point (an existing "first" point).
            else:
                if (track_point_counter == 1):
                
### (Begin) This condition suggests an orphaned point(the existing -1/"prev_" point)
                    out_orphaned_records.write("" + str(track_index) + "," + str(int(prev_timeval)) + "," + prev_datetimetoken + "," + prev_msgid + "," + prev_mmsi + "," + prev_navstatus + "," + prev_sog + "," + prev_cog + "," + prev_tr_hdg + "," + prev_latitude + "," + prev_longitude + "," + prev_pos_acc)
### (End) This condition suggests an orphaned point(the existing -1 "prev_" point)
                        
            out_vessel_records.close()
            out_stationary_records.close()
            out_invalid_records.close()
            out_orphaned_records.close()

def generate_threshold_GIS(segment_split_directory, gis_directory, out_filename_prefix, outputEPSG):
    
    # Copy the output filename from the argument vector.
    out_line_filename = os.path.basename(gis_directory + sep + out_filename_prefix + "_lines")
    out_line_directory = os.path.dirname(gis_directory + sep + out_filename_prefix + "_lines")
    out_point_filename = os.path.basename(gis_directory + sep + out_filename_prefix + "_points")
    out_point_directory = os.path.dirname(gis_directory + sep + out_filename_prefix + "_points") 

    # Set up the shapefile driver.
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Create the data source.
    track_data_source = driver.CreateDataSource(out_line_directory)
    point_data_source = driver.CreateDataSource(out_point_directory)

    # create the spatial reference, WGS84
    in_srs = osr.SpatialReference()
    in_srs.ImportFromEPSG(inputEPSG)

    # Create the output spatial reference, BC Albers
    # Presuming effective equality between NAD83 / WGS84 over function scope (e.g. North America -- https://www.packtpub.com/books/content/working-geo-spatial-data-python)
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(outputEPSG)

    # Create a transform object to project WGS84 coordinates to user-specified output.
    transform = osr.CoordinateTransformation(in_srs, out_srs)
    
    # Create the track and point layers.
    track_layer = track_data_source.CreateLayer(out_line_filename, out_srs, ogr.wkbLineString)
    if track_layer is None:
        print "\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting."
        quit()

    point_layer = point_data_source.CreateLayer(out_point_filename, out_srs, ogr.wkbPoint)
    if point_layer is None:
        print "\nError encountered when creating output point shapefile: " + out_point_directory + "\\" + out_point_filename + " \nAborting."
        quit()

    # Define the data fields to be included in the output track layer.
    track_layer.CreateField(ogr.FieldDefn("TrackID", ogr.OFTInteger))
    track_field_mmsi = ogr.FieldDefn("mmsi", ogr.OFTString)
    track_field_mmsi.SetWidth(12)
    track_layer.CreateField(track_field_mmsi)
    track_layer.CreateField(ogr.FieldDefn("elp_sec", ogr.OFTInteger))
    track_field_st_date = ogr.FieldDefn("st_date", ogr.OFTString)
    track_field_st_date.SetWidth(20)
    track_layer.CreateField(track_field_st_date)
    track_field_en_date = ogr.FieldDefn("en_date", ogr.OFTString)
    track_field_en_date.SetWidth(20)
    track_layer.CreateField(track_field_en_date)
    track_layer.CreateField(ogr.FieldDefn("bad_spd", ogr.OFTInteger))

    # Define the data fields to be included in the output point layer.
    point_layer.CreateField(ogr.FieldDefn("TrackID", ogr.OFTInteger))
    point_layer.CreateField(ogr.FieldDefn("msgid", ogr.OFTInteger))
    point_field_mmsi = ogr.FieldDefn("mmsi", ogr.OFTString)
    point_field_mmsi.SetWidth(12)
    point_layer.CreateField(point_field_mmsi)
    point_field_navstatus = ogr.FieldDefn("navstatus", ogr.OFTString)
    point_field_navstatus.SetWidth(28) ### This is to accomodate the gpsd interpreted text for nav status, might be better to truncate or re-enumerate for size.
    point_layer.CreateField(point_field_navstatus)
    point_field_sog = ogr.FieldDefn("sog", ogr.OFTReal)
    point_field_sog.SetWidth(5)
    point_field_sog.SetPrecision(1)
    point_layer.CreateField(point_field_sog)
    point_field_cog = ogr.FieldDefn("cog", ogr.OFTReal)
    point_field_cog.SetWidth(5)
    point_field_cog.SetPrecision(1)
    point_layer.CreateField(point_field_cog)
    point_field_tr_hdg = ogr.FieldDefn("tr_hdg", ogr.OFTReal)
    point_field_tr_hdg.SetWidth(5)
    point_field_tr_hdg.SetPrecision(1)
    point_layer.CreateField(point_field_tr_hdg)
    point_layer.CreateField(ogr.FieldDefn("pos_acc", ogr.OFTInteger))
    point_field_ais_date = ogr.FieldDefn("ais_date", ogr.OFTString)
    point_field_ais_date.SetWidth(20)
    point_layer.CreateField(point_field_ais_date)
    point_layer.CreateField(ogr.FieldDefn("bad_spd", ogr.OFTInteger))
       
    # Print a message about the stage of processing
    print "\nGenerating thresholded track GIS layer generation."
    
    #Establish a progressbar for iterating over incoming split files
    in_segment_split_files_bar = progressbar.ProgressBar()
        
    # Iterate over the divided files generated in the previous step.
    for segment_split_filename in in_segment_split_files_bar(glob(segment_split_directory  + sep + "*.txt")):

        with open(segment_split_filename,'r') as in_track_records:

            # Initialize an index into the track ID values and a list to hold waypoints while tracks are constructed.
            prev_inTrackID = -1

            track_wkt = 'LINESTRING ('
            track_len = 0
            
            # Iterate over the lines in the current file of track data.
            for line in in_track_records:
                
                # Split the incoming csv line.
                tokenizedline = line.split(',')
                inTrackID = int(tokenizedline[0])

                # If the current line is the first of the input file, initialize variables.
                if(prev_inTrackID == -1):
                    
                    prev_inTrackID = inTrackID
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMinSeconds = int(tokenizedline[1])
                    inMaxSeconds = inMinSeconds
                    inPrevSeconds = inMinSeconds
                    inMinDate = tokenizedline[2]
                    inMaxDate = inMinDate
                    inMsgId = tokenizedline[3]
                    inMMSI = tokenizedline[4]
                    inNavStatus = tokenizedline[5]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])     
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Initialize a flag to store whether or not questionable implided speeds were encountered for a given track.
                    badSpeedFlag = 0 
                    
                    # Append the current point to the list of points for the track.
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    
                    # Generate a feature and and populate its fields.
                    point_feature = ogr.Feature(point_layer.GetLayerDefn())
                    point_feature.SetField("TrackID" ,inTrackID)
                    point_feature.SetField("msgid" ,inMsgId)
                    point_feature.SetField("mmsi" ,inMMSI)
                    point_feature.SetField("navstatus" ,inNavStatus)
                    point_feature.SetField("sog" ,inSog)
                    point_feature.SetField("cog" ,inCog)
                    point_feature.SetField("tr_hdg" ,inTrHdg)
                    point_feature.SetField("pos_acc" ,inPosAcc)
                    point_feature.SetField("ais_date" ,inMaxDate) # Note: value in inMaxDate always valid for points.
                    point_feature.SetField("bad_spd" ,badSpeedFlag)

                    # Create the point geometry, project it and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
                    # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                    # (assuming datum shift is inconsequential {NA}.)
                    point_obj.Transform(transform)
                    point_feature.SetGeometry(point_obj)
                    
                    # Create the feature within the output layer, then reclaim assigned memory.
                    point_layer.CreateFeature(point_feature)
                    point_feature.Destroy()
                    
                # If the current line is the first of a new track, terminate the existing track 
                # before initializing the next.
                elif(prev_inTrackID <> inTrackID):
                    
                    # If the existing track has at least two points, proceed with writing it out.
                    if(track_len > 1):
                    
                        # Generate a feature and and populate its fields.
                        track_feature = ogr.Feature(track_layer.GetLayerDefn())
                        track_feature.SetField("TrackID" ,prev_inTrackID)
                        track_feature.SetField("mmsi" ,inMMSI)
                        track_feature.SetField("elp_sec" ,(inMaxSeconds - inMinSeconds))
                        track_feature.SetField("st_date" , inMinDate)
                        track_feature.SetField("en_date" , inMaxDate)
                        track_feature.SetField("bad_spd" , badSpeedFlag)

                        # Close the track geometry and assign it to the feature.
                        track_wkt = track_wkt + ")"
                        
                        track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                        
                        # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                        # (assuming datum shift is inconsequential {NA}.)
                        track_obj.Transform(transform)
                        
                        track_feature.SetGeometry(track_obj)
                        
                        # Create the feature within the output layer, then reclaim assigned memory.
                        track_layer.CreateFeature(track_feature)
                        track_feature.Destroy()
                    
                    prev_inTrackID = inTrackID
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMinSeconds = int(tokenizedline[1])
                    inMaxSeconds = inMinSeconds
                    inPrevSeconds = inMinSeconds
                    inMinDate = tokenizedline[2]
                    inMaxDate = inMinDate
                    inMsgId = tokenizedline[3]
                    inMMSI = tokenizedline[4]
                    inNavStatus = tokenizedline[5]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Reinitialize the flag to store whether or not questionable implided speeds were encountered for a given track.
                    badSpeedFlag = 0 
                    
                    # Reset the existing list of points / append the current point to the list of points for the next track.
                    track_wkt = 'LINESTRING (' + str(inLon) + " " + str(inLat)
                    track_len = 1
                    
                    ### (Begin) Add the current point to the point output layer
                    
                    # Generate a feature and and populate its fields.
                    point_feature = ogr.Feature(point_layer.GetLayerDefn())
                    point_feature.SetField("TrackID" ,inTrackID)
                    point_feature.SetField("msgid" ,inMsgId)
                    point_feature.SetField("mmsi" ,inMMSI)
                    point_feature.SetField("navstatus" ,inNavStatus)
                    point_feature.SetField("sog" ,inSog)
                    point_feature.SetField("cog" ,inCog)
                    point_feature.SetField("tr_hdg" ,inTrHdg)
                    point_feature.SetField("pos_acc" ,inPosAcc)
                    point_feature.SetField("ais_date" ,inMaxDate) # Note: value in inMaxDate always valid for points.
                    point_feature.SetField("bad_spd" ,badSpeedFlag)
                    
                    # Create the point geometry, project it and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
                    # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                    # (assuming datum shift is inconsequential {NA}.)
                    point_obj.Transform(transform)
                    point_feature.SetGeometry(point_obj)
                    
                    # Create the feature within the output layer, then reclaim assigned memory.
                    point_layer.CreateFeature(point_feature)
                    point_feature.Destroy()
                     ### (End) Add the current point to the point output layer
                    
                # If the current line extends the current track, aggregate the values as required.
                else:
                    
                    # Copy the previous timestamp and position values.
                    inPrevSeconds = inMaxSeconds
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMaxSeconds = int(tokenizedline[1])
                    inMaxDate = tokenizedline[2]
                    inMsgId = tokenizedline[3]
                    inMMSI = tokenizedline[4]
                    inNavStatus = tokenizedline[5]
                    inSog = parseSOG(tokenizedline[6])
                    inCog = parseCOG(tokenizedline[7])
                    inTrHdg = parseTrHdg(tokenizedline[8])
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    
                    # If the distance and time between the previous and current position indicates a speed of 
                    # greater than 160 kph (0.044444 km per s), set a 'bad speed' flag on the record.
                    timeDelta = inMaxSeconds - inPrevSeconds
                    distanceDelta = haversine_coords_km(inPrevLon, inPrevLat, inLon, inLat)
                    if (timeDelta > 0) and ((distanceDelta / timeDelta) > 0.0444444444):
                        
                        badSpeedFlag = 1
                        
                    # Append the current point to the list of points for the track.
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    
                    # Generate a feature and and populate its fields.
                    point_feature = ogr.Feature(point_layer.GetLayerDefn())
                    point_feature.SetField("TrackID" ,inTrackID)
                    point_feature.SetField("msgid" ,inMsgId)
                    point_feature.SetField("mmsi" ,inMMSI)
                    point_feature.SetField("navstatus" ,inNavStatus)
                    point_feature.SetField("sog" ,inSog)
                    point_feature.SetField("cog" ,inCog)
                    point_feature.SetField("tr_hdg" ,inTrHdg)
                    point_feature.SetField("pos_acc" ,inPosAcc)
                    point_feature.SetField("ais_date" ,inMaxDate) # Note: value in inMaxDate always valid for points.
                    point_feature.SetField("bad_spd" ,badSpeedFlag)
                    
                    # Create the point geometry, project it and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
                    # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                    # (assuming datum shift is inconsequential {NA}.)
                    point_obj.Transform(transform)
                    point_feature.SetGeometry(point_obj)
                    
                    # Create the feature within the output layer, then reclaim assigned memory.
                    point_layer.CreateFeature(point_feature)
                    point_feature.Destroy()
                     ### (End) Add the current point to the point output layer

            # If the last remaining track has at least two points, proceed with writing it out.
            if(track_len > 1):
                
                # Generate a feature and and populate its fields.
                track_feature = ogr.Feature(track_layer.GetLayerDefn())
                track_feature.SetField("TrackID" ,prev_inTrackID)
                track_feature.SetField("mmsi" ,inMMSI)
                track_feature.SetField("elp_sec" ,(inMaxSeconds - inMinSeconds))
                track_feature.SetField("st_date" , inMinDate)
                track_feature.SetField("en_date" , inMaxDate)
                track_feature.SetField("bad_spd" , badSpeedFlag)

                # Close the track geometry and assign it to the feature.
                track_wkt = track_wkt + ")"
                
                track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                
                # Project the feature object to BC Albers / NAD83 from WGS84 unprojected 
                # (assuming datum shift is inconsequential {NA}.)
                track_obj.Transform(transform)
                
                track_feature.SetGeometry(track_obj)
                
                # Create the feature within the output layer, then reclaim assigned memory.
                track_layer.CreateFeature(track_feature)
                track_feature.Destroy()

    # Destroy the data sources to flush features to disk.
    point_data_source.Destroy()
    track_data_source.Destroy()

#########################################################
        
# Usage string for the script.
usage_string = """Usage: 1_generate_tracks_from_MEOPAR_AIS.py output_directory output_filename_prefix inputfilename outputEPSG short_track_indicator 0:[track_separation_time max_point_speed]|1:[max_elapsed_time]

Splits pre-parsed, position-referenced AIS records (Postgres Export) by message, then mmsi (vessel), then segment and generates GIS output layers. Presumes that the outputdirectory can be created, but does not yet exist. Requires GDAL / OGR/OSR, and progressbar2 packages for Python. Developed in support of the NEMES project (http://www.nemesproject.com/) and the exactEarth SAIS data initiative of MEOPAR (http://www.meopar.ca/).

output_directory - The target directory for the output results to be generated.
output_filename_prefix - The base filename under which the output should be written
inputfilename - The location/name of the input file of formatted AIS data (as exported from the MEOPAR/eE/Dal database).
outputEPSG - The EPSG identifier for the output projection / coordinate system of the GIS data.
short_track_indicator - A flag, set as 1 to indicate point to point segment generation, and 0 for time and speed based track generation.
track_separation_time (only for short_track_indicator == 0) - For track generation, the maximum time interval (in seconds) to be permitted between subsequent points in a track.
max_point_speed (only for short_track_indicator == 0) - For track generation, the maximum speed allowed (in kph), over which points with such speed are dropped as erroneous.
max_elapsed_time (only for short_track_indicator == 1) - For segment generation, the maximum length of segment (in seconds) to be considered valid.

"""

def main():

    # If at least five arguments are not provided, display an usage message.
    if (len(sys.argv) < 6):
        print usage_string
        quit()

    # Retrieve the output directory, filename prefix, input filename and output EPSG.
    cmd_outdirectory = sys.argv[1]
    cmd_out_filename_prefix = sys.argv[2]
    cmd_in_filename = sys.argv[3]
    cmd_outputEPSG = int(sys.argv[4])

    # Establish the output sub-directories required for the process:
    split_file_foldername = "01_mmsi_and_msg_split"
    message_split_file_subfoldername = "01_message_split"
    other_split_file_subfoldername = "02_other_split"
    segment_split_foldername = "02_segment_split"
    other_segment_split_subfoldername = "01_other_split"
    gis_output_foldername = "03_gis_output"

    # Build fully qualified directories for the sub-directories.
    cmd_split_file_directory = os.path.join(cmd_outdirectory, split_file_foldername)
    cmd_split_message_file_directory = os.path.join(cmd_outdirectory, split_file_foldername, message_split_file_subfoldername)
    cmd_split_other_file_directory = os.path.join(cmd_outdirectory, split_file_foldername, other_split_file_subfoldername)
    cmd_segment_split_directory = os.path.join(cmd_outdirectory, segment_split_foldername)
    cmd_segment_other_split_directory = os.path.join(cmd_outdirectory, segment_split_foldername, other_segment_split_subfoldername)
    cmd_gis_directory = os.path.join(cmd_outdirectory, gis_output_foldername)

    # Set the indicator for short (point-to-point tracks vs threshold-split tracks).
    short_track_indicator = int(sys.argv[5])

    # If the short track indicator is not valid (i.e. 0 or 1), display an usage message and abort.
    if not(short_track_indicator == 0 or short_track_indicator == 1):
        print "\nInvalid short_track_indicator value, aborting."
        print usage_string
        quit()
        
    # If the indicator is valid, check that the number of arguments are appropriate to the indicator.
    else:
        if (short_track_indicator == 1):
            if (len(sys.argv) <> 7):
                print "\nIncorrect number of arguments for specified short_track_indicator value, aborting."
                print usage_string
                quit()
                
            else:
                # Copy value for max elapsed time between points for segment to be 
                # created (seconds) Satellite overflight for eE circa 2015 cited as 
                # <= 90 minutes 5400 -- 7200 considered a generous buffer, was old 
                # default (2016-04-01 to 2016-08-15).
                #cmd_max_elapsed_time = 7200
                cmd_max_elapsed_time = float(sys.argv[6])

        elif (short_track_indicator == 0):
            if (len(sys.argv) <> 8):
                print "\nIncorrect number of arguments for specified short_track_indicator value, aborting."
                print usage_string
                quit()
                
            # If the argument count is correct, parse the 
            else:
                cmd_track_separation_time = float(sys.argv[6])
                cmd_max_point_speed = float(sys.argv[7])

    # If the specified input file does not exist, abort and display an usage message.
    if not os.path.isfile(cmd_in_filename):
        print "Input file (" + cmd_in_filename + ")not found.\n"
        print usage_string
        quit()

    # If the specified output directory already exists, abort and display an error message.
    if os.path.exists(cmd_outdirectory):
        print "\nSpecified output directory already exists, aborting."
        quit()
        
    else:

        # Attempt to create the output sub-directories required for the process.
        try:
            os.makedirs(cmd_outdirectory)
            os.makedirs(cmd_split_file_directory)
            os.makedirs(cmd_split_message_file_directory)
            os.makedirs(cmd_split_other_file_directory)
            os.makedirs(cmd_segment_split_directory)
            os.makedirs(cmd_segment_other_split_directory)
            os.makedirs(cmd_gis_directory)

        except:
            print "\nError creating output directories, aborting."
            quit()
            
    # Split the track data provided into separate text files based first on message type, then by mmsi.
    split_pre_tracks(cmd_in_filename, cmd_split_message_file_directory, cmd_split_file_directory, cmd_split_other_file_directory, cmd_out_filename_prefix)
    
    # If short tracks are indicated, then proceed with the appropriate type 
    # of split on the per-mmsi files, then generate the GIS representations.
    if(short_track_indicator):

        print "\nShort (point-to-point) track generation."

        # Generate the text representation of the segments.
        generate_short_segments(cmd_split_file_directory, cmd_segment_split_directory, cmd_segment_other_split_directory, cmd_max_elapsed_time)
        
        # Interpret the text representation of the segments as a GIS polyline layers.
        generate_short_GIS(cmd_segment_split_directory, cmd_gis_directory, cmd_out_filename_prefix, cmd_max_elapsed_time, cmd_outputEPSG)

    # If thresholded tracks are indicated, then proceed with the appropriate type 
    # of split on the per-mmsi files, then generate the GIS representations.
    else:
            
        # Print a message about the stage of processing
        print "\nThresholded track generation."
        
        # Generate the text representation of the tracks.
        generate_threshold_tracks(cmd_split_file_directory, cmd_segment_split_directory, cmd_segment_other_split_directory, cmd_track_separation_time, cmd_max_point_speed)
    
        # Interpret the text representation of the tracks as GIS polyline and point shapefile layers.
        generate_threshold_GIS(cmd_segment_split_directory, cmd_gis_directory, cmd_out_filename_prefix, cmd_outputEPSG)

# If we're invoked directly (which we generally expect), run.
if __name__ == "__main__":
    main()

                    


