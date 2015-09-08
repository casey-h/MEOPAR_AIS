#!/usr/bin/python

# Import qgis core libraries
from qgis.core import *
from PyQt4.QtCore import *

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
from datetime import datetime,timedelta
from math import radians, cos, sin, asin, sqrt, degrees, atan2, pi

def haversine(lon1, lat1, lon2, lat2):

    # Calculations translated from Movable Type (http://www.movable-type.co.uk/scripts/latlong.html)

    #Calculate the great circle distance between two points 
    #on the earth (specified in decimal degrees)
    
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2.0)**2 + cos(lat1) * cos(lat2) * sin(dlon/2.0)**2
    c = 2.0 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth
    km = 6367.0 * c
    return km 

def densify_time_midpoint(lon1, lat1, time_start, lon2, lat2, time_end, threshold_time):

    # Return an array of coordinates densified along the line between two 
    # given coordinates, down to a specified threshold_time value. Input 
    # coordinates presumed to be decimal latitude / longitude; times as
    # Unix timestamps, and threshold in seconds.
    
    # Calculate the elapsed time in seconds, between the two timestamps.
    elapsed_delta = time_end - time_start
    
    ##### DEBUG
    #print "Elapsed Seconds Between Points:" + str(elapsed_delta.total_seconds()) + "\n"
    ##### DEBUG
    
    # If the difference in time between the start and end coordinates is
    # less than the specified threshold, return an 2d list comprising the 
    # input coordinates.
    if(elapsed_delta.total_seconds() <= threshold_time):
    
        return [[lon1, lat1, time_start],[lon2,lat2,time_end]]
    
    # If the difference in time is greater than the threshold, then
    # compute the midpoint between the two points, and concatenate it 
    # between densified coordinate arrays to the sides of the midpoint.
    else:
    
        # Calculate the midpoint between the points in space.
        [mid_lon, mid_lat] = midpoint(lon1, lat1, lon2, lat2)
        midpoint_delta = timedelta(0, round(elapsed_delta.total_seconds() / 2))
        midpoint_time = time_start + midpoint_delta
        
        # Return the concatenation of the densified coordinates to the sides 
        # of the midpoint.
### Should this index be -2? -- No, when indexing this way, the shown index is the first element excluded.
        return densify_time_midpoint(lon1, lat1, time_start, mid_lon, mid_lat, midpoint_time, threshold_time)[:-1] + [[mid_lon, mid_lat,midpoint_time]] + densify_time_midpoint(mid_lon, mid_lat, midpoint_time, lon2, lat2, time_end, threshold_time)[1:]

def midpoint(lon1, lat1, lon2, lat2):

    # Calculations translated from Movable Type (http://www.movable-type.co.uk/scripts/latlong.html)

    # Convert decimal degrees to radians 
    lambda_one, phi_one, lambda_two, phi_two = map(radians, [lon1, lat1, lon2, lat2])

    Bx = cos(phi_two) * cos(lambda_two-lambda_one);
    By = cos(phi_two) * sin(lambda_two-lambda_one);
    phi_three = atan2(sin(phi_one) + sin(phi_two),
    sqrt((cos(phi_one)+Bx)*(cos(phi_one)+Bx) + By*By ));
    lambda_three = lambda_one + atan2(By, cos(phi_one) + Bx);
    
    lon3, lat3 = map(degrees, [lambda_three, phi_three])
    
    return [lon3, lat3]
    
def densify_dist_midpoint(lon1, lat1, time_start, lon2, lat2, time_end, threshold_distance):

    # Return an array of coordinates densified along the line between two 
    # given coordinates, down to a specified threshold_distance value. Input 
    # coordinates presumed to be decimal latitude / longitude; times as
    # Unix timestamps, and threshold in kilometres.
    
    # Calculate the distance between the two incoming points.
    separation_distance = haversine(lon1, lat1, lon2, lat2)
    
    # Calculate the elapsed time in seconds, between the two timestamps.
    elapsed_delta = time_end - time_start
    
    # If the distance between the start and end coordinates is less
    # less than the specified threshold, return an 2d list comprising the 
    # input coordinates.
    if(separation_distance <= threshold_distance):
    
        return [[lon1, lat1, time_start],[lon2,lat2,time_end]]
    
    # If the difference in time is greater than the threshold, then
    # compute the midpoint between the two points, and concatenate it 
    # between densified coordinate arrays to the sides of the midpoint.
    else:
    
        # Calculate the midpoint between the points in space.
        [mid_lon, mid_lat] = midpoint(lon1, lat1, lon2, lat2)
        midpoint_delta = timedelta(0, round(elapsed_delta.total_seconds() / 2))
        midpoint_time = time_start + midpoint_delta
        
        # Return the concatenation of the densified coordinates to the sides 
        # of the midpoint.
### Should this index be -2? -- No, when indexing this way, the shown index is the first element excluded.
        return densify_dist_midpoint(lon1, lat1, time_start, mid_lon, mid_lat, midpoint_time, threshold_distance)[:-1] + [[mid_lon, mid_lat,midpoint_time]] + densify_dist_midpoint(mid_lon, mid_lat, midpoint_time, lon2, lat2, time_end, threshold_distance)[1:]

def densify_dist_iter(lon1, lat1, time_start, lon2, lat2, time_end, threshold_distance):

    # Return an array of coordinates densified along the line between two 
    # given coordinates, down to a specified threshold_distance value using
    # iteration rather than bisection. Input coordinates presumed to be 
    # decimal latitude / longitude; times as Unix timestamps, and threshold in kilometres.
    
    # Calculate the distance between the two incoming points.
    separation_distance = haversine(lon1, lat1, lon2, lat2)
    
    # Calculate the elapsed time in seconds, between the two timestamps.
    elapsed_delta = time_end - time_start
    
    # If the distance between the start and end coordinates is less
    # less than the specified threshold, return an 2d list comprising the 
    # input coordinates.
    if(separation_distance <= threshold_distance):
    
        return [[lon1, lat1, time_start],[lon2, lat2, time_end]]
    
    # If the difference in time is greater than the threshold, then
    # compute an intermediate point between the two points, extended along the
    # line between the two points at a distance equal to the given threshold 
    # and concatenate the start point to the densified coordinate arrays from
    # the generated point onward.
    else:
    
        # Calculate the midpoint between the points in space.
        [proj_lon, proj_lat] = projected_coordinate(lon1, lat1, lon2, lat2, threshold_distance)
        proj_delta = timedelta(0, float(elapsed_delta.total_seconds()) * float(threshold_distance) / float(separation_distance))
        proj_time = time_start + proj_delta
        
        # Return the concatenation of the projected coordinates to the densification
        # of the remaining portion of the line.
        return [[lon1, lat1, time_start]] + densify_dist_iter(proj_lon, proj_lat, proj_time, lon2, lat2, time_end, threshold_distance)

def projected_coordinate(lon1, lat1, lon2, lat2, distance):

    # Calculations translated from Movable Type (http://www.movable-type.co.uk/scripts/latlong.html)
    
    # Convert the incoming coordinates to radians.
    lambda_one, phi_one, lambda_two, phi_two = map(radians, [lon1, lat1, lon2, lat2])
    
    # Calculate the bearing between the two points
    y = sin(lambda_two - lambda_one) * cos(phi_two)
    x = cos(phi_one) * sin(phi_two) - (sin(phi_one) * cos(phi_two) * cos(lambda_two - lambda_one))
    brng_deg = degrees(atan2(y, x))
    brng = atan2(y, x)
    
    # Calculate distance proportion, using a fixed value for the Earth's radius in units of
    # distance to be utilized (km)
    dist_prop = float(distance) / float(6367)
    
    # Calculate the target point, following the bearing from the start to end point.
    phi_three = asin( sin(phi_one) * cos(dist_prop) + (cos(phi_one) * sin(dist_prop) * cos(brng)))
    lambda_three = lambda_one + atan2(sin(brng) * sin(dist_prop) * cos(phi_one), cos(dist_prop) - (sin(phi_one) * sin(phi_three)))
    
    # Convert the coordinate calculated to degrees
    [lon3, lat3] = map(degrees, [lambda_three, phi_three])

    return [lon3, lat3]
    
def great_circ_intermed(lon1, lat1, lon2, lat2, distance_interval, total_distance=-1):

    # Calculation from Ed Williams Aviation Formulary V1.46 (http://williams.best.vwh.net/avform.htm#Intermediate)
    
    # Cast coordinates to radians.
    rad_lon1, rad_lat1, rad_lon2, rad_lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
   
    #If the total distance is not specified, calculate using Haversine.
    if(total_distance < 0):
        total_distance = haversine(lon1, lat1, lon2, lat2)
        
    # Calculate the distance fraction to be travelled.
    distance_fraction = float(distance_interval) / float(total_distance)

    # Convert distance to radians.
    distance_radians=(pi/(180*60))*total_distance
    
    #A=sin((1-f)*d)/sin(d)
    #valA = sin((1.0-distance_fraction)*total_distance)/sin(total_distance)
    valA = sin((1.0-distance_fraction)*distance_radians)/sin(distance_radians)
    
    #B=sin(f*d)/sin(d)
    #valB= sin(distance_fraction*total_distance) / sin(total_distance)
    valB= sin(distance_fraction*distance_radians) / sin(distance_radians)
    
    #x = A*cos(lat1)*cos(lon1) +  B*cos(lat2)*cos(lon2)
    valX =valA*cos(rad_lat1)*cos(rad_lon1) + valB*cos(rad_lat2)*cos(rad_lon2)
    
    #y = A*cos(lat1)*sin(lon1) +  B*cos(lat2)*sin(lon2)
    valY = valA*cos(rad_lat1)*sin(rad_lon1) + valB*cos(rad_lat2)*sin(rad_lon2)
    
    #z = A*sin(lat1)           +  B*sin(lat2)
    valZ = valA*sin(rad_lat1) + valB*sin(rad_lat2)
    
    # lat=atan2(z,sqrt(x^2+y^2))
    rad_outlat = atan2(valZ, sqrt(valX*valX + valY*valY))
    
    #lon=atan2(y,x)
    rad_outlon = atan2(valY, valX)
    
    outlon, outlat = map(degrees, [rad_outlon, rad_outlat])
    
    return [outlon, outlat]

from math import floor
    
def densify_dist_iter_mk2(lon1, lat1, time_start, lon2, lat2, time_end, threshold_distance):

    # Return an array of coordinates densified along the line between two 
    # given coordinates, down to a specified threshold_distance value using
    # iteration rather than bisection. Input coordinates presumed to be 
    # decimal latitude / longitude; times as Unix timestamps, and threshold in kilometres.
    
    # Calculate the distance between the two incoming points.
    separation_distance = haversine(lon1, lat1, lon2, lat2)
    
    # Calculate the elapsed time in seconds, between the two timestamps.
    elapsed_delta = time_end - time_start
    
    # If the distance between the start and end coordinates is less
    # less than the specified threshold, return an 2d list comprising the 
    # input coordinates.
    if(separation_distance <= threshold_distance):
    
        return [[lon1, lat1, time_start],[lon2, lat2, time_end]]
    
    # If the difference in time is greater than the threshold, then
    # compute intermediate points between the source coordinates, extended along the
    # line between the two points at a distance equal to the given threshold 
    # and concatenate the start point to the densified coordinate arrays from
    # the generated point onward.
    else:
    
        # Calculate the midpoint between the points in space.
        [proj_lon, proj_lat] = great_circ_intermed(lon1, lat1, lon2, lat2, threshold_distance, separation_distance)
        
        ##### DEBUG:
        #print "Source: " + str(lon1) + "," + str(lat1) + "Intermed: " + str(proj_lon) + "," + str(proj_lat) + "Dest: " + str(lon2) + "," + str(lat2)
        ##### DEBUG:
        
        proj_delta = timedelta(0, float(elapsed_delta.total_seconds()) * float(threshold_distance) / float(separation_distance))
        proj_time = time_start + proj_delta
        
        # Return the concatenation of the projected coordinates to the densification
        # of the remaining portion of the line.
        return [[lon1, lat1, time_start]] + densify_dist_iter_mk2(proj_lon, proj_lat, proj_time, lon2, lat2, time_end, threshold_distance)
    
#########################################################

# Requires access to QGIS python resources, e.g:
# export PYTHONPATH=/qgispath/share/qgis/python
# export LD_LIBRARY_PATH=/qgispath/lib

# supply path to where is your qgis installed
QgsApplication.setPrefixPath("/usr/share/qgis", True)

# load providers
QgsApplication.initQgis()

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) < 3):
    print 'Usage: build_tracklines_NEMES.py outputshapefileprefix inputfilename [inputfilename ...] ... \n Reads the specified input filenames, presumed to correspond to csv files of waypoint data, one per mmsi, ordered by timestamp. (output of divide_tracks_v_NEMES.py).\n'
    quit()

# Copy the output filename from the argument vector.
out_line_filename = sys.argv[1] + "_lines.shp"
out_point_filename = sys.argv[1] + "_points.shp"
out_int_point_filename = sys.argv[1] + "_int_points.shp"

# Define the data fields to be included in the output track layer.
trackdatafields = QgsFields()
trackdatafields.append(QgsField("TrackID", QVariant.Int, "", 20))
trackdatafields.append(QgsField("mmsi", QVariant.String, "", 12))
trackdatafields.append(QgsField("elp_sec", QVariant.Int, "", 20))
trackdatafields.append(QgsField("st_date", QVariant.String, "", 20))
trackdatafields.append(QgsField("en_date", QVariant.String, "", 20))
trackdatafields.append(QgsField("bad_spd", QVariant.Int, "", 20))

# Define the data fields to be included in the output point layer.
pointdatafields = QgsFields()
pointdatafields.append(QgsField("TrackID", QVariant.Int, "", 20))
pointdatafields.append(QgsField("msgid", QVariant.Int, "", 20))
pointdatafields.append(QgsField("mmsi", QVariant.String, "", 12))
pointdatafields.append(QgsField("navstatus", QVariant.String, "", 28)) ### This is to accomodate the gpsd interpreted text for nav status, might be better to truncate or re-enumerate for size.
pointdatafields.append(QgsField("sog", QVariant.Double, "", 5, 1))
pointdatafields.append(QgsField("cog", QVariant.Double, "", 5, 1))
pointdatafields.append(QgsField("tr_hdg", QVariant.Double, "", 5, 1))
pointdatafields.append(QgsField("pos_acc", QVariant.Int, "", 20))
pointdatafields.append(QgsField("ais_date", QVariant.String, "", 20))
pointdatafields.append(QgsField("hydro_dist", QVariant.Double, "", 12, 6))
pointdatafields.append(QgsField("bad_spd", QVariant.Int, "", 20))

# Define the data fields to be included in the output interpolated point
# layer.
intpointdatafields = QgsFields()
intpointdatafields.append(QgsField("TrackID", QVariant.Int, "", 20))
intpointdatafields.append(QgsField("mmsi", QVariant.String, "", 12))
intpointdatafields.append(QgsField("ais_date", QVariant.String, "", 20))
intpointdatafields.append(QgsField("hydro_dist", QVariant.Double, "", 12, 6))
intpointdatafields.append(QgsField("interp_pt", QVariant.Int, "", 20))

# Hydrophone Coordinate
hydro_lon = -135.3050
hydro_lat = 53.3055

# Interpolation/densification resolution (kilometres)
interp_res = 5

# Instantiate the output file writer objects.
trackwriter = QgsVectorFileWriter(out_line_filename,"CP1250",trackdatafields,QGis.WKBLineString,None,"ESRI Shapefile")
pointwriter = QgsVectorFileWriter(out_point_filename,"CP1250",pointdatafields,QGis.WKBPoint,None,"ESRI Shapefile")
intpointwriter = QgsVectorFileWriter(out_int_point_filename,"CP1250",intpointdatafields,QGis.WKBPoint,None,"ESRI Shapefile")

# If an error occurs while creating the output file writer objects, display the error and abort.
if trackwriter.hasError() != QgsVectorFileWriter.NoError:
    print "Error when creating shapefile: ", trackwriter.hasError()
    quit()
if pointwriter.hasError() != QgsVectorFileWriter.NoError:
    print "Error when creating shapefile: ", pointwriter.hasError()
    quit()
if intpointwriter.hasError() != QgsVectorFileWriter.NoError:
    print "Error when creating shapefile: ", intpointwriter.hasError()
    quit()
    
# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 2):

    
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        #print("Processing: " + sys.argv[(2 + infile_index)])
        
        #with open("/home/casey/storage/AIS_Parsing/parse_results_tracklines_2013/C/205250000.txt",'r') as in_track_records:
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
                    
                    if ((tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "None") or (tokenizedline[8] == "")):
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
                    trackPoints.append(QgsPoint(inLon, inLat))
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    fet = QgsFeature()
                    fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(inLon, inLat)))
                    fet.initAttributes(11)
                    fet.setAttribute(0, inTrackID)
                    fet.setAttribute(1, inMsgId)
                    fet.setAttribute(2, inMMSI)
                    fet.setAttribute(3, inNavStatus)
                    fet.setAttribute(4, inSog)
                    fet.setAttribute(5, inCog)
                    fet.setAttribute(6, inTrHdg)
                    fet.setAttribute(7, inPosAcc)
                    fet.setAttribute(8, inMaxDate) # Note: value in inMaxDate always valid for points.
                    fet.setAttribute(9, hydro_dist)
                    fet.setAttribute(10, badSpeedFlag)
                    pointwriter.addFeature(fet)
                     ### (End) Add the current point to the point output layer
                    
                # If the current line is the first of a new track, terminate the existing track 
                # before initializing the next.
                elif(prev_inTrackID <> inTrackID):
                    
                    # If the existing track has at least two points, proceed with writing it out.
                    if(len(trackPoints) > 1):
                    
                        fet = QgsFeature()
                        fet.setGeometry(QgsGeometry.fromPolyline(trackPoints))
                        fet.initAttributes(6)
                        fet.setAttribute(0, (prev_inTrackID))
                        fet.setAttribute(1, (inMMSI))
                        fet.setAttribute(2, (inMaxSeconds - inMinSeconds))
                        fet.setAttribute(3, (inMinDate))
                        fet.setAttribute(4, (inMaxDate))
                        fet.setAttribute(5, (badSpeedFlag))
                        trackwriter.addFeature(fet)
                    
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
                    
                    if ((tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "None") or (tokenizedline[8] == "")):
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
                    trackPoints.append(QgsPoint(inLon, inLat))
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    fet = QgsFeature()
                    fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(inLon, inLat)))
                    fet.initAttributes(11)
                    fet.setAttribute(0, inTrackID)
                    fet.setAttribute(1, inMsgId)
                    fet.setAttribute(2, inMMSI)
                    fet.setAttribute(3, inNavStatus)
                    fet.setAttribute(4, inSog)
                    fet.setAttribute(5, inCog)
                    fet.setAttribute(6, inTrHdg)
                    fet.setAttribute(7, inPosAcc)
                    fet.setAttribute(8, inMaxDate) # Note: value in inMaxDate always valid for points.
                    fet.setAttribute(9, hydro_dist)
                    fet.setAttribute(10, badSpeedFlag)
                    pointwriter.addFeature(fet)
                     ### (End) Add the current point to the point output layer
                    
                # If the current line extends the current track, aggregate the values as required.
                else:
                    
                    # Copy the previous timestamp and position values.
                    inPrevSeconds = inMaxSeconds
                    inPrevLat = inLat
                    inPrevLon = inLon
                    
    ################ (begin) Calculate the interpolated points to correspond to the current track segment.

                    # Start interpolation at the prior 'max date'
                    interp_start_str = inMaxDate
                    
                    # Start interpolation coordinates.
                    interp_start_lon = inPrevLon
                    interp_start_lat = inPrevLat
                    
                    # End interpolation at the current date
                    interp_end_str = tokenizedline[2]
                    
                    # End interpolation coordinates.
                    interp_end_lon = float(tokenizedline[10])
                    interp_end_lat = float(tokenizedline[9])
                    
                    # Create datetime representations of the bounding dates
                    interp_start = datetime(int(interp_start_str[0:4]),int(interp_start_str[4:6]),int(interp_start_str[6:8]),int(interp_start_str[9:11]),int(interp_start_str[11:13]),int(interp_start_str[13:15]))
                    interp_end = datetime(int(interp_end_str[0:4]),int(interp_end_str[4:6]),int(interp_end_str[6:8]),int(interp_end_str[9:11]),int(interp_end_str[11:13]),int(interp_end_str[13:15]))
                    
                    # Compute a linear distance-based interpolation/densification 
                    # between the indicated positions.
                    
                    #[interp_points_lon, interp_points_lat, interp_dates] = densify_dist_iter(interp_start_lon, interp_start_lat, interp_start, interp_end_lon, interp_end_lat, interp_end, interp_res)
                    interp_points = densify_dist_iter(interp_start_lon, interp_start_lat, interp_start, interp_end_lon, interp_end_lat, interp_end, interp_res)
                    
    ################ (end) Calculate the interpolated points to correspond to the current track segment.
                    
                    # Parse the values from the input line.
                    # Adjusted field set: 20150817
                    inMaxSeconds = int(tokenizedline[1])
                    inMaxDate = tokenizedline[2]
                    inMsgId = tokenizedline[3]
                    inMMSI = tokenizedline[4]
                    inNavStatus = tokenizedline[5]
                    
                    if ((tokenizedline[6] == "None") or (tokenizedline[6] == "")):
                        inSog = float(102.3)
                    else:
                        inSog = float(tokenizedline[6])
                    
                    if ((tokenizedline[7] == "None") or (tokenizedline[7] == "")):
                        inCog = float(360.0)
                    else:
                        inCog = float(tokenizedline[7])
                    
                    if ((tokenizedline[8] == "None") or (tokenizedline[8] == "")):
                        inTrHdg = float(511.0)
                    else:
                        inTrHdg = float(tokenizedline[8])
                        
                    inLat = float(tokenizedline[9])
                    inLon = float(tokenizedline[10])
                    inPosAcc = tokenizedline[11]
                    
                    # If the distance and time between the previous and current positions.
                    timeDelta = inMaxSeconds - inPrevSeconds
                    distanceDelta = haversine(inPrevLon, inPrevLat, inLon, inLat)
                    #if (timeDelta > 0) and ((distanceDelta / (timeDelta / 3600)) > 120):
                    #if (timeDelta > 0) and ((distanceDelta / timeDelta) > 0.0333333333):
                    if (timeDelta > 0) and ((distanceDelta / timeDelta) > 0.0444444444):
                        
                        badSpeedFlag = 1
                        
                    # Append the current point to the list of points for the track.
                    trackPoints.append(QgsPoint(inLon, inLat))
                   
    ############### (begin) Add the interpolated point data to the output layer.
                    
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(interp_points[0][0], interp_points[0][1], hydro_lon, hydro_lat)
                    fet = QgsFeature()
                    fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(interp_points[0][0], interp_points[0][1])))
                    fet.initAttributes(5)
                    fet.setAttribute(0, (prev_inTrackID))
                    fet.setAttribute(1, (inMMSI))
                    fet.setAttribute(2, (interp_points[0][2].strftime("%Y%m%d_%H%M%S")))
                    fet.setAttribute(3, (hydro_dist))
                    fet.setAttribute(4, (-1))   # Non-interpolated point
                    intpointwriter.addFeature(fet)
                    
                    for interp_value in interp_points[1:-1]:
                        
                        hydro_dist = haversine(interp_value[0], interp_value[1], hydro_lon, hydro_lat)
                        fet = QgsFeature()
                        fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(interp_value[0], interp_value[1])))
                        fet.initAttributes(5)
                        fet.setAttribute(0, (prev_inTrackID))
                        fet.setAttribute(1, (inMMSI))
                        fet.setAttribute(2, (interp_value[2].strftime("%Y%m%d_%H%M%S")))
                        fet.setAttribute(3, (hydro_dist))
                        fet.setAttribute(4, (1))    # Interpolated point
                        intpointwriter.addFeature(fet)
                    
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(interp_points[-1][0], interp_points[-1][1], hydro_lon, hydro_lat)
                    fet = QgsFeature()
                    fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(interp_points[-1][0], interp_points[-1][1])))
                    fet.initAttributes(5)
                    fet.setAttribute(0, (prev_inTrackID))
                    fet.setAttribute(1, (inMMSI))
                    fet.setAttribute(2, (interp_points[-1][2].strftime("%Y%m%d_%H%M%S")))
                    fet.setAttribute(3, (hydro_dist))
                    fet.setAttribute(4, (-1))   # Non-interpolated point
                    intpointwriter.addFeature(fet)
                    
    ############### (end) Add the interpolated point data to the output layer.
                   
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
                    fet = QgsFeature()
                    fet.setGeometry(QgsGeometry.fromPoint(QgsPoint(inLon, inLat)))
                    fet.initAttributes(11)
                    fet.setAttribute(0, inTrackID)
                    fet.setAttribute(1, inMsgId)
                    fet.setAttribute(2, inMMSI)
                    fet.setAttribute(3, inNavStatus)
                    fet.setAttribute(4, inSog)
                    fet.setAttribute(5, inCog)
                    fet.setAttribute(6, inTrHdg)
                    fet.setAttribute(7, inPosAcc)
                    fet.setAttribute(8, inMaxDate) # Note: value in inMaxDate always valid for points.
                    fet.setAttribute(9, hydro_dist)
                    fet.setAttribute(10, badSpeedFlag)
                    pointwriter.addFeature(fet)
                    ### (End) Add the current point to the point output layer

            # If the last remaining track has at least two points, proceed with writing it out.
            if(len(trackPoints) > 1):
                
                fet = QgsFeature()
                fet.setGeometry(QgsGeometry.fromPolyline(trackPoints))
                
                fet.initAttributes(6)
                fet.setAttribute(0, (prev_inTrackID))
                fet.setAttribute(1, (inMMSI))
                fet.setAttribute(2, (inMaxSeconds - inMinSeconds))
                fet.setAttribute(3, (inMinDate))
                fet.setAttribute(4, (inMaxDate))
                fet.setAttribute(5, (badSpeedFlag))
                trackwriter.addFeature(fet)

# Delete the writers to flush features to disk.
del trackwriter
del pointwriter
del intpointwriter
