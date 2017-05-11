#!/usr/bin/python

# Import fiona
import fiona
from shapely.geometry import mapping, Point, LineString

import sys
from glob import glob

# build_tracklines_and_points_fiona_NEMES.py - Converts a set of input files, as 
# generated / formatted by divide_tracks_v_NEMES.py, and generates a GIS layer 
# (ESRI # Shapefile) of track segments (as polyline objects), using the 
# combination of MMSI number and track ID (added by divide_tracks_v_NEMES.py) 
# as a primary key, and assuming the points are in chronological order. 
# Requires that fiona and shapely (+GDAL) be installed in the calling 
# environment and be available to python. Flags tracklines which contain 
# point sequences with implied speeds that suggest there are bad points 
# contained within.

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

# Define the data fields to be included in the output track layer.
track_data_source_driver = 'ESRI Shapefile'
track_data_crs = {'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 'proj': 'longlat'}
track_data_schema = {'geometry':'LineString',
'properties': {'TrackID': 'int',
'mmsi': 'str:12',
'elp_sec': 'int',
'st_date': 'str:20',
'en_date': 'str:20',
'bad_spd': 'int' }}

# Define the data fields to be included in the output point layer.
point_data_source_driver = 'ESRI Shapefile'
point_data_crs = {'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 'proj': 'longlat'}
point_data_schema = {'geometry':'Point',
'properties': {'TrackID': 'int',
'msgid': 'int',
'mmsi': 'str:12',
'navstatus' : 'str:28', 
'sog': 'float:5.1',
'cog': 'float:5.1',
'tr_hdg': 'float:5.1',
'pos_acc': 'int',
'ais_date' : 'str:20',
'hydro_dist': 'float:12.6',
'bad_spd': 'int'}}
### navstatus length is to accomodate the gpsd interpreted text for nav status, might be better to truncate or re-enumerate for size.

# Counters to determine the number of track and point records written.
track_counter = 0
point_counter = 0

# The interval at which records should be flushed to the output files.
flush_interval = 5000

# Hydrophone Coordinate
hydro_lon = -135.3050
hydro_lat = 53.3055

# If an error occurs while creating the output file writer objects, display the error and abort.
try:
    track_outfile = fiona.open(out_line_filename, 'w', crs=track_data_crs, driver=track_data_source_driver, schema=track_data_schema)
except Exception, e:
    print "Error when creating output track shapefile: " + out_line_filename
    print "Message" + str(e)

try:
    point_outfile = fiona.open(out_point_filename, 'w', crs=point_data_crs, driver=point_data_source_driver, schema=point_data_schema)
except Exception, e:
    print "Error when creating output track shapefile: " + out_line_filename
    print "Message" + str(e)
    
# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 2):
   
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        # DEBUG
        print("Processing: " + in_filename)
        # DEBUG
        
        with open(in_filename,'r') as in_track_records:
        
            # Initialize an index into the track ID values and a list to hold waypoints while tracks are constructed.
            prev_inTrackID = -1
            trackPoints = []
            
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
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)

                    # Define and write the point record to file.
                    point_outfile.write({
                        'geometry': mapping(Point(inLon, inLat)),
                        'properties': {
                        'TrackID': inTrackID,
                        'msgid': inMsgId,
                        'mmsi': inMMSI,
                        'navstatus': inNavStatus,
                        'sog': inSog,
                        'cog': inCog,
                        'tr_hdg': inTrHdg,
                        'pos_acc': inPosAcc,
                        'ais_date': inMaxDate,
                        'hydro_dist': hydro_dist,
                        'bad_spd': badSpeedFlag
                        }
                    })
                    
                    # Increment the counter and flush if needed.
                    point_counter = point_counter + 1
                        
                    if (point_counter % flush_interval == 0):
                        point_outfile.flush()
                    
                    out_point_record = None
                    
                    ####
                    # Append the current point to the list of points for the track.
                    
                    trackPoints.append((inLon, inLat))
                    
                     ### (End) Add the current point to the point output layer
                    
                # If the current line is the first of a new track, terminate the existing track 
                # before initializing the next.
                elif(prev_inTrackID <> inTrackID):
                    
                    # If the existing track has at least two points, proceed with writing it out.
                    if(len(trackPoints) > 1):
                    
                        # Define and write the track record to file.
                        track_outfile.write({
                            'geometry': mapping(LineString(trackPoints)),
                            'properties': {
                            'TrackID': prev_inTrackID,
                            'mmsi': inMMSI,
                            'elp_sec': (inMaxSeconds - inMinSeconds),
                            'st_date': inMinDate,
                            'en_date': inMaxDate,
                            'bad_spd': badSpeedFlag
                            }
                        })
                        
                        # Increment the counter and flush if needed.
                        track_counter = track_counter + 1
                        
                        if (track_counter % flush_interval == 0):
                            track_outfile.flush()

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
                    trackPoints = []
                    trackPoints.append((inLon, inLat))
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    # Define and write the point record to file.
                    point_outfile.write({
                        'geometry': mapping(Point(inLon, inLat)),
                        'properties': {
                        'TrackID': inTrackID,
                        'msgid': inMsgId,
                        'mmsi': inMMSI,
                        'navstatus': inNavStatus,
                        'sog': inSog,
                        'cog': inCog,
                        'tr_hdg': inTrHdg,
                        'pos_acc': inPosAcc,
                        'ais_date': inMaxDate,
                        'hydro_dist': hydro_dist,
                        'bad_spd': badSpeedFlag
                        }
                    })
                    
                    # Increment the counter and flush if needed.
                    point_counter = point_counter + 1
                        
                    if (point_counter % flush_interval == 0):
                        point_outfile.flush()
                    
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
                    trackPoints.append((inLon, inLat))
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    # Define and write the point record to file.
                    point_outfile.write({
                        'geometry': mapping(Point(inLon, inLat)),
                        'properties': {
                        'TrackID': inTrackID,
                        'msgid': inMsgId,
                        'mmsi': inMMSI,
                        'navstatus': inNavStatus,
                        'sog': inSog,
                        'cog': inCog,
                        'tr_hdg': inTrHdg,
                        'pos_acc': inPosAcc,
                        'ais_date': inMaxDate,
                        'hydro_dist': hydro_dist,
                        'bad_spd': badSpeedFlag
                        }
                    })
                    
                    # Increment the counter and flush if needed.
                    point_counter = point_counter + 1
                    
                    if (point_counter % flush_interval == 0):
                        point_outfile.flush()
                    
                     ### (End) Add the current point to the point output layer

            # If the last remaining track has at least two points, proceed with writing it out.
            if(len(trackPoints) > 1):
                
                # Define and write the track record to file.
                track_outfile.write({
                    'geometry': mapping(LineString(trackPoints)),
                    'properties': {
                    'TrackID': prev_inTrackID,
                    'mmsi': inMMSI,
                    'elp_sec': (inMaxSeconds - inMinSeconds),
                    'st_date': inMinDate,
                    'en_date': inMaxDate,
                    'bad_spd': badSpeedFlag
                    }
                })
            
                # Increment the counter and flush if needed.
                track_counter = track_counter + 1
                
                if (track_counter % flush_interval == 0):
                    track_outfile.flush()

# Close the output files to flush features to disk.
track_outfile.close()
point_outfile.close()
