#!/usr/bin/python

# Import OGR/OSR
import osgeo.ogr as ogr
import osgeo.osr as osr

import sys
import os.path
from glob import glob


# build_tracklines_and_points_gdal_NEMES.py

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
out_line_filename = os.path.basename(sys.argv[1] + "_lines")
out_line_directory = os.path.dirname(sys.argv[1] + "_lines")
out_point_filename = os.path.basename(sys.argv[1] + "_points")
out_point_directory = os.path.dirname(sys.argv[1] + "_points") 

# Set up the shapefile driver.
driver = ogr.GetDriverByName("ESRI Shapefile")

# Create the data source.
track_data_source = driver.CreateDataSource(out_line_directory)
point_data_source = driver.CreateDataSource(out_point_directory)

# create the spatial reference, WGS84
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)

# Create the track and point layers.

track_layer = track_data_source.CreateLayer(out_line_filename, srs, ogr.wkbLineString)
if track_layer is None:
    print "\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting."
    quit()

point_layer = point_data_source.CreateLayer(out_point_filename, srs, ogr.wkbPoint)
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
point_field_hydro_dist = ogr.FieldDefn("hydro_dist", ogr.OFTReal)
point_field_hydro_dist.SetWidth(12)
point_field_hydro_dist.SetPrecision(6)
point_layer.CreateField(point_field_hydro_dist)
point_layer.CreateField(ogr.FieldDefn("bad_spd", ogr.OFTInteger))

# Hydrophone Coordinate
hydro_lon = -135.3050
hydro_lat = 53.3055
    
# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 2):
   
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        with open(in_filename,'r') as in_track_records:
        
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
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
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
                    point_feature.SetField("hydro_dist" ,hydro_dist)
                    point_feature.SetField("bad_spd" ,badSpeedFlag)
                    
                    # Create the point geometry and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
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
                    
                    # Reset the existing list of points / append the current point to the list of points for the next track.
                    track_wkt = 'LINESTRING (' + str(inLon) + " " + str(inLat)
                    track_len = 1
                    
                    ### (Begin) Add the current point to the point output layer
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
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
                    point_feature.SetField("hydro_dist" ,hydro_dist)
                    point_feature.SetField("bad_spd" ,badSpeedFlag)
                    
                    # Create the point geometry and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
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
                    if (track_len > 0):
                        track_wkt = track_wkt + ", " + str(inLon) + " " + str(inLat)
                    else:
                        track_wkt = track_wkt + str(inLon) + " " + str(inLat)
                    track_len = track_len + 1
                    
                    # Calculate the distance from the point to the hydrophone.
                    hydro_dist = haversine(inLon, inLat, hydro_lon, hydro_lat)
                    
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
                    point_feature.SetField("hydro_dist" ,hydro_dist)
                    point_feature.SetField("bad_spd" ,badSpeedFlag)
                    
                    # Create the point geometry and assign it to the feature.
                    point_wkt = "POINT(%f %f)" % (inLon, inLat)
                    point_obj = ogr.CreateGeometryFromWkt(point_wkt)
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
                track_feature.SetGeometry(track_obj)
                
                # Create the feature within the output layer, then reclaim assigned memory.
                track_layer.CreateFeature(track_feature)
                track_feature.Destroy()

# Destroy the data sources to flush features to disk.
point_data_source.Destroy()
track_data_source.Destroy()
