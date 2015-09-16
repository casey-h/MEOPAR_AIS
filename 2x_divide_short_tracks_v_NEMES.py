#!/usr/bin/python

# divide_short_tracks_v_NEMES.py - Takes as input a number of input text filenames, 
# presumed to be of the format generated by split_underway_mmsi_NEMES.py. 
# Generates as output, triplets of files per input file, under the specified 
# output directory. Output filenames are generated based on the input 
# filenames:
#    Suffix "_stationary" - Includes rows from the input data deemed to 
#       represent stationary points.
#    Suffix "_invalid" - Includes rows from the input data deemed to include 
#       invalid / unrecognized data.
#    No suffix - Includes rows from the input data, prefixed with track ID 
#       numbers, defining discrete track segments from within the sequences 
#       of input data points. Suitable for use in script 
#       build_tracklines_NEMES.py
#
# LIMITATIONS: The process for rejecting positions is fairly naive and 
# simplistic. It cannot cope with multiple (>2) sequential bad positions, 
# and it has no ability to identify situations where more than one bad 
# position is noted at the beginning of a trajectory segment. Also, the 
# threshold values used to divide segments and eliminate points should 
# not be taken as firmly established values, rather they are just values 
# which have resulted in subjectively "reasonable" results in the past.
# 
# This is a revised version of divide_tracks_v_NEMES.py, which generates
# only segment pairs, and which also calculates and stores directions
# with each segment.

from glob import glob
import sys
import time
from os import sep

#########################################################
from math import radians, cos, sin, asin, sqrt, atan2, pi

def haversine(lon1, lat1, lon2, lat2):

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
    
def bearing(lon1, lat1, lon2, lat2):

    #Calculate bearing traveled from one set of 
    #coordinates to reach a second.
    # Drawn from calculations at: http://www.movable-type.co.uk/scripts/latlong.html
    
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
    
#########################################################

# Establish a path separator. 
path_separator = sep

#DEBUG
print path_separator

if (len(sys.argv) < 3):
    print 'Usage: divide_short_tracks_v_NEMES.py outputdirectory inputfilename [inputfilename ...] ... \n Adds track delineation to a number of input files of vessel underway data, predivided into single files per mmsi under the specified output directory.\n'
    quit()

outdirectory = sys.argv[1]

# track_separation_time - Maximum time between sequential data points for which the points will be a valid segment. Measured in seconds. 
# Note: 180s = max separation between sequential type A AIS points under any conditions according to AIS spec.
track_separation_time = 180

for infile_index in range(len(sys.argv) - 2):
    
    # Attempt wildcard expansion on any input file specified (Only (likely) req'd for Windows).
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        print("Processing: " + in_filename)

        (dummyone, dummytwo, outfilename) = in_filename.rpartition(path_separator)

        try:
            out_vessel_records = open(outdirectory + path_separator + outfilename, 'w')
        except IOError:
            print "Error opening file: " + outdirectory + path_separator + outfilename + "\n"
            quit()

        try:
            out_stationary_records = open(outdirectory + path_separator + "stationary_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + outdirectory + path_separator + "stationary_" + outfilename + "\n"
            quit()
            
        try:
            out_invalid_records = open(outdirectory + path_separator + "invalid_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + outdirectory + path_separator + "invalid_" + outfilename + "\n"
            quit()

        with open(in_filename,'r') as in_vessel_records:
            
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
                    
                    # If the current point is the second of a track segment, calculate the 
                    # speed and direction, and write out the pair to the output.
                    elif(track_point_counter == 1):
                        
                        # Calculate the elapsed time between the points.
                        elapsed_segment_time = timeval - prev_timeval
                        
                        # Calculate the distance between the positions.
                        segment_length = haversine(float(prev_longitude), float(prev_latitude), float(longitude), float(latitude))
                        
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
