#!/usr/bin/python

# divide_tracks.py - Takes as input a number of input text filenames, 
# presumed to be of the format generated by split_ee_AIS_pre_tracks.py or 
# split_NM4_Sourced_AIS_pre_tracks.py. Generates as output, quartets of 
# files per input file, under the specified output directory. Output 
# filenames are generated based on the input 
# filenames:
#    Suffix "_stationary" - Includes rows from the input data deemed to 
#       represent stationary points.
#    Suffix "_invalid" - Includes rows from the input data deemed to include 
#       invalid / unrecognized data.
#    Suffix "_orphaned" - Includes rows from the input data dropped due to  
#       temporal or speed difference from adjacent points.
#    No suffix - Includes rows from the input data, prefixed with track ID 
#       numbers, defining discrete track segments from within the sequences 
#       of input data points. Suitable for use in script 
#       build_tracklines_and_points.py
#
# LIMITATIONS: The process for rejecting positions is fairly naive and 
# simplistic. It cannot cope with multiple (>2) sequential bad positions, 
# and it has no ability to identify situations where more than one bad 
# position is noted at the beginning of a trajectory segment. Also, the 
# threshold values used to divide segments and eliminate points should 
# not be taken as firmly established values, rather they are just values 
# which have resulted in subjectively "reasonable" results in the past.
# 

from glob import glob
import sys
import time
from os import sep

#########################################################
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):

    #Calculate the great circle distance between two points 
    #on the earth (specified in decimal degrees)
    
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
#########################################################

# Establish a path separator. 
path_separator = sep

if (len(sys.argv) < 5):
    print 'Usage: divide_tracks.py trackseparationtime maxpointspeed outputdirectory inputfilename [inputfilename ...] ... \n Adds track delineation to a number of input files of vessel underway data, predivided into single files per mmsi under the specified output directory. \n trackseparationtime - measured in seconds; max allowed time between successive points in same segment \n maxpointspeed - measured in kph; max allowed implied speed between points; points in excess dropped \n outputDeveloped in support of the NEMES project (http://www.nemesproject.com/).\n'
    quit()

# track_separation_time - Maximum time between sequential data points for which the points will be considered part of the same track segment. 
# Measured in seconds. Initially 180s max separation between sequential points under any conditions according to AIS spec.
# Changed to 8 hours as maximum reasonable interval for SKBS area, considering 201108 and 201202 (2015-08-21 ), then moved to arg
# to improve utility / transparency of operation
track_separation_time = int(sys.argv[1])

# track_speed_threshold - Value for maximum speed considered feasible, measured in kilometres per second to avoid unit conversion. Used to identify when points are to be dropped from consideration because of excessive implied speed (suggesting bad position fix).
# CH - Move this VV to arg to improve utility / transparency of operation
max_point_speed = float(sys.argv[2])

#track_speed_threshold = 0.0444444444
# Convert the max speed value in kph to kilometres per second to match with script.
track_speed_threshold = max_point_speed / float(3600)


# Store the output directory.
outdirectory = sys.argv[3]

for infile_index in range(len(sys.argv) - 4):
    
    # Attempt wildcard expansion on any input file specified (Only (likely) req'd for Windows).
    for in_filename in glob(sys.argv[(4 + infile_index)]):
    
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
            
        try:
            out_orphaned_records = open(outdirectory + path_separator + "orphaned_" + outfilename, 'w')
        except IOError:
            print "Error opening file: " + outdirectory + path_separator + "orphaned_" + outfilename + "\n"
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
                            dist_one = haversine(float(prev_prev_longitude), float(prev_prev_latitude), float(prev_longitude), float(prev_latitude))
                            if (dist_one == 0):
                                spd_one = 0
                            else:
                                ela_time_one = float(prev_timeval) - float(prev_prev_timeval)
                                if(ela_time_one > 0):
                                    spd_one = dist_one / ela_time_one
                                else:
                                    spd_one = 999
                            
                            dist_two = haversine(float(prev_prev_longitude), float(prev_prev_latitude), float(longitude), float(latitude))
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
                    prev_prev_dist = haversine(float(prev_prev_longitude), float(prev_prev_latitude), float(prev_longitude), float(prev_latitude))
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