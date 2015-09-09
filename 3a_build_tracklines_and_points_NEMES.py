#!/usr/bin/python

# Import qgis core libraries
from qgis.core import *
from PyQt4.QtCore import *

import sys
from glob import glob

# build_tracklines_and_points_NEMES.py - Converts a set of input files, as 
# generated / formatted by divide_tracks_v_NEMES.py, and generates a GIS layer 
# (ESRI # Shapefile) of track segments (as polyline objects), using the 
# combination of MMSI number and track ID (added by divide_tracks_v_NEMES.py) 
# as a primary key, and assuming the points are in chronological order. 
# Requires that QGIS be installed in the calling environment and be available
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

# Hydrophone Coordinate
hydro_lon = -135.3050
hydro_lat = 53.3055

# Instantiate the output file writer objects.
trackwriter = QgsVectorFileWriter(out_line_filename,"CP1250",trackdatafields,QGis.WKBLineString,None,"ESRI Shapefile")
pointwriter = QgsVectorFileWriter(out_point_filename,"CP1250",pointdatafields,QGis.WKBPoint,None,"ESRI Shapefile")


# If an error occurs while creating the output file writer object, display the error and abort.
if trackwriter.hasError() != QgsVectorFileWriter.NoError:
    print "Error when creating shapefile: ", trackwriter.hasError()
    quit()
if pointwriter.hasError() != QgsVectorFileWriter.NoError:
    print "Error when creating shapefile: ", pointwriter.hasError()
    quit()
    
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
