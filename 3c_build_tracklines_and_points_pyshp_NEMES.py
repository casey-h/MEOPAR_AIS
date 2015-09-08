#!/usr/bin/python

# Import pyshp
import shapefile

import sys
from glob import glob

# build_tracklines_NEMES.py - Converts a set of input files, as generated /
# formatted by divide_tracks_v_NEMES.py, and generates a GIS layer (ESRI 
# Shapefile) of track segments (as polyline objects), using the combination 
# of MMSI number and track ID (added by divide_tracks_v_NEMES.py) as a 
# primary key, and assuming the points are in chronological order. Requires
# that QGIS be installed in the calling environment and be available
# to python via pyqgis. Flags tracklines which contain point sequences 
# with implied speeds that suggest there are bad points contained within.

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

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) < 3):
    print 'Usage: build_tracklines_NEMES.py outputshapefileprefix inputfilename [inputfilename ...] ... \n Reads the specified input filenames, presumed to correspond to csv files of waypoint data, one per mmsi, ordered by timestamp. (output of divide_tracks_v_NEMES.py).\n'
    quit()

# Copy the output filename from the argument vector.
out_line_filename = sys.argv[1] + "_lines.shp"
out_point_filename = sys.argv[1] + "_points.shp"

# Instantiate an output track layer.
trackwriter = shapefile.Writer(shapefile.POLYLINE)

# Force auto-balancing between geometry and records.
trackwriter.autobalance = 1

# Define the data fields to be included in the output track layer.
trackwriter.field('TrackID', 'N', 16, 0)
trackwriter.field('mmsi', 'C', 12)
trackwriter.field('elp_sec', 'N', 16, 0)
trackwriter.field('st_date', 'C', 20)
trackwriter.field('en_date', 'C', 20)
trackwriter.field('bad_spd', 'N', 16, 0)

# Instantiate an output point layer.
pointwriter = shapefile.Writer(shapefile.POINT)

# Force auto-balancing between geometry and records.
pointwriter.autobalance = 1

# Define the data fields to be included in the output point layer.
pointwriter.field('TrackID', 'N', 16, 0)
pointwriter.field('msgid', 'N', 16, 0)
pointwriter.field('mmsi', 'C', 12)
pointwriter.field('navstatus', 'C', '28') ### This is to accomodate the gpsd interpreted text for nav status, might be better to truncate or re-enumerate for size.
pointwriter.field('sog', 'N', 5, 1)
pointwriter.field('cog', 'N', 5, 1)
pointwriter.field('tr_hdg', 'N', 5, 1)
pointwriter.field('pos_acc', 'N', 16, 0)
pointwriter.field('ais_date', 'C', 20)
pointwriter.field('hydro_dist', 'N' , 16, 6)
pointwriter.field('bad_spd', 'N', 16, 0)

# Hydrophone Coordinate
hydro_lon = -135.3050
hydro_lat = 53.3055

#########################
# Iteration and output record counters.
iteration_count = 0
point_count = 0
track_count = 0

# Set an interval at which output records are flushed.
flush_interval = 50000
#########################


# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 2):
   
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        #print("Processing: " + sys.argv[(2 + infile_index)])
        with open(in_filename,'r') as in_track_records:
        
            ### DEBUG
            iteration_count = iteration_count + 1
            if (iteration_count % 1 == 0):
                print "Iteration: " + str(iteration_count)
            ### DEBUG
        
            # Initialize an index into the track ID values and a list to hold waypoints while tracks are constructed.
            prev_inTrackID = -1
            trackPoints = [[]]
            
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
                    
                    if ((tokenizedline[6] == "n/a") or (tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        if tokenizedline[6] == "fast":
                            inSog = float(102.2)
                        else:
                            inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "n/a") or (tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "n/a") or (tokenizedline[8] == "None") or (tokenizedline[8] == "")):
                        inTrHdg = float(511.0)
                    else:
                        inTrHdg = float(tokenizedline[8])
                        
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Initialize a flag to store whether or not questionable implided speeds were encountered for a given track.
                    badSpeedFlag = 0 
                    
                    # Append the current point to the list of points for the track.
                    
                    #trackPoints.append(QgsPoint(inLon, inLat))
                    trackPoints[0].append([float(inLon),float(inLat)])
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    #########################
                    # Increment the output point counter
                    point_count = point_count + 1
                    
                    # Every flush_interval output points, save the point file and reopen for editing.
                    if(point_count % flush_interval == 0):
                        pointwriter.save(out_point_filename)
                        pointwriter = None
                        pointwriter = shapefile.Editor(shapefile=out_point_filename)
                    #########################
                    
                    pointwriter.point(float(inLon), float(inLat))
                    pointwriter.record(int(inTrackID), int(inMsgId), inMMSI, inNavStatus, float(inSog), float(inCog), float(inTrHdg), int(inPosAcc), inMaxDate, hydro_dist, int(badSpeedFlag))
                    # Note: value in inMaxDate always valid for points.
                    
                    ### (End) Add the current point to the point output layer
                    
                # If the current line is the first of a new track, terminate the existing track 
                # before initializing the next.
                elif(prev_inTrackID <> inTrackID):
                    
                    # If the existing track has at least two points, proceed with writing it out.
                    #if(len(trackPoints) > 1):
                    if(len(trackPoints[0]) > 1):
                    
                        #########################
                        # DEBUG
                        #print trackPoints
                        #print "\n"
                        # DEBUG
                        
                        # Increment the output track counter
                        track_count = track_count + 1
                        
                        # Every flush_interval output tracks, save the track file and reopen for editing.
                        if(track_count % flush_interval == 0):
                            trackwriter.save(out_line_filename)
                            trackwriter = None
                            trackwriter = shapefile.Editor(shapefile=out_line_filename)
                        #########################
                        
                        trackwriter.line(parts=trackPoints)
                        trackwriter.record(int(prev_inTrackID), inMMSI, (inMaxSeconds - inMinSeconds), inMinDate, inMaxDate, int(badSpeedFlag))
                    
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
                    
                    if ((tokenizedline[6] == "n/a") or (tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        if tokenizedline[6] == "fast":
                            inSog = float(102.2)
                        else:
                            inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "n/a") or (tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "n/a") or (tokenizedline[8] == "None") or (tokenizedline[8] == "")):
                        inTrHdg = float(511.0)
                    else:
                        inTrHdg = float(tokenizedline[8])
                        
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
                    # Reinitialize the flag to store whether or not questionable implided speeds were encountered for a given track.
                    badSpeedFlag = 0 
                    
                    # Reset the existing list of points, then append the current point to the list of points for the next track.
                    trackPoints = [[]]
                    #trackPoints.append(QgsPoint(inLon, inLat))
                    trackPoints[0].append([float(inLon),float(inLat)])
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    #########################
                    # Increment the output point counter
                    point_count = point_count + 1
                    
                    # Every flush_interval output points, save the point file and reopen for editing.
                    if(point_count % flush_interval == 0):
                        pointwriter.save(out_point_filename)
                        pointwriter = None
                        pointwriter = shapefile.Editor(shapefile=out_point_filename)
                    #########################
                    
                    pointwriter.point(float(inLon), float(inLat))
                    pointwriter.record(int(inTrackID), int(inMsgId), inMMSI, inNavStatus, float(inSog), float(inCog), float(inTrHdg), int(inPosAcc), inMaxDate, hydro_dist, int(badSpeedFlag))
                    # Note: value in inMaxDate always valid for points.
                    
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
                    
                    if ((tokenizedline[6] == "n/a") or (tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        if tokenizedline[6] == "fast":
                            inSog = float(102.2)
                        else:
                            inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "n/a") or (tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "n/a") or (tokenizedline[8] == "None") or (tokenizedline[8] == "")):
                        inTrHdg = float(511.0)
                    else:
                        inTrHdg = float(tokenizedline[8])
                        
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    
                    # If the distance and time between the previous and current positions.
                    timeDelta = inMaxSeconds - inPrevSeconds
                    distanceDelta = haversine(inPrevLon, inPrevLat, inLon, inLat)
                    if (timeDelta > 0) and ((distanceDelta / timeDelta) > 0.0444444444):
                        
                        badSpeedFlag = 1
                        
                    # Append the current point to the list of points for the track.
                    #trackPoints.append(QgsPoint(inLon, inLat))
                    trackPoints[0].append([float(inLon),float(inLat)])
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    #########################
                    # Increment the output point counter
                    point_count = point_count + 1
                    
                    # Every flush_interval output points, save the point file and reopen for editing.
                    if(point_count % flush_interval == 0):
                        pointwriter.save(out_point_filename)
                        pointwriter = None
                        pointwriter = shapefile.Editor(shapefile=out_point_filename)
                    #########################
                    
                    pointwriter.point(float(inLon), float(inLat))
                    pointwriter.record(int(inTrackID), int(inMsgId), inMMSI, inNavStatus, float(inSog), float(inCog), float(inTrHdg), int(inPosAcc), inMaxDate, hydro_dist, int(badSpeedFlag))
                    # Note: value in inMaxDate always valid for points.
                    ### (End) Add the current point to the point output layer

            # If the last remaining track has at least two points, proceed with writing it out.
            #if(len(trackPoints) > 1):
            if(len(trackPoints[0]) > 1):
                
                # DEBUG
                #print trackPoints
                #print "\n"
                # DEBUG
                
                #########################
                # Increment the output track counter
                track_count = track_count + 1
                
                # Every flush_interval output tracks, save the track file and reopen for editing.
                if(track_count % flush_interval == 0):
                    trackwriter.save(out_line_filename)
                    trackwriter = None
                    trackwriter = shapefile.Editor(shapefile=out_line_filename)
                #########################

                
                trackwriter.line(parts=trackPoints)
                trackwriter.record(int(prev_inTrackID), inMMSI, (inMaxSeconds - inMinSeconds), inMinDate, inMaxDate, int(badSpeedFlag))

# Flush features to disk.
trackwriter.save(out_line_filename)
pointwriter.save(out_point_filename)
