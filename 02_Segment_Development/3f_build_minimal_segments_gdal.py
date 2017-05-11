#!/usr/bin/python

# build_minimal_segments_gdal.py - Assumes specified target SRID has units of metres and 
# spheroid equivalent to WGS84, alternately 4326 (WGS84) can be specified to use haversine
# formula estimate of distance.

# Import OGR/OSR
import osgeo.ogr as ogr
import osgeo.osr as osr

import sys
import os.path
from glob import glob

#########################################################
from math import radians, cos, sin, asin, sqrt

def haversine(inGeometry):

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
#########################################################

# Key script "constants" :
# Input spatial reference (EPSG#) - WGS84
inputEPSG = 4326

#Minimum inferred speed boundary (knots 2016-04-01)
min_speed_bound_kts = 1

#Maximum inferred speed boundary (knots 2016-04-01)
max_speed_bound_kts = 86.3930885411603

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) < 5):
    print 'Usage: build_minimal_segments_gdal.py outputEPSG maxTime outputshapefileprefix inputfilename [inputfilename ...] ... \n Reads the specified input filenames, presumed to correspond to csv files of waypoint data, one per mmsi, ordered by timestamp. (output of divide_tracks.py). Generates shapefiles of corresponding trajectories. Output EPSG is the target for the generated files (e.g. BC Albers - 3005; UTM 20N NAD83 - 2961). Assumes specified target SRID has units of metres and spheroid equivalent to WGS84, alternately 4326 (WGS84) can be specified to use haversine formula estimate of distance. Input is presumed to be WGS84 (AIS default). Max time is the maximum temporal separation to be allowed between points (default was 7200 to 2016-08-15, when added as arg). Developed in support of the NEMES project (http://www.nemesproject.com/).\n'
    quit()

# Copy the output EPSG from the agrument vector
#Output spatual reference (EPSG#)
#outputEPSG = 3005 # - BC Albers
#outputEPSG = 2961 # UTM 20N NAD83 (NS / Hfx)
outputEPSG = int(sys.argv[1])
    
# Copy value for max elapsed time between points for segment to be 
# created (seconds) Satellite overflight for eE circa 2015 cited as 
# <= 90 minutes 5400 -- 7200 considered a generous buffer, was old 
# default (2016-04-01 to 2016-08-15).
#max_elapsed_time = 7200
max_elapsed_time = int(sys.argv[2])
    
# Copy the output filename from the argument vector.
out_line_filename = os.path.basename(sys.argv[3] + "_lines")
out_line_directory = os.path.dirname(sys.argv[3] + "_lines")

# Set up the shapefile driver.
driver = ogr.GetDriverByName("ESRI Shapefile")

# Create the data source.
track_data_source = driver.CreateDataSource(out_line_directory)

# create the spatial reference, WGS84
in_srs = osr.SpatialReference()
in_srs.ImportFromEPSG(inputEPSG)

# Create the output spatial reference, BC Albers
# Presuming effective euqality betewwn NAD83 / WGS84 for North America (e.g. https://www.packtpub.com/books/content/working-geo-spatial-data-python)
out_srs = osr.SpatialReference()
out_srs.ImportFromEPSG(outputEPSG)

# Create a transform object to project WGS84 coordinates to BC Albers 
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
    
# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 4):
   
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(4 + infile_index)]):
    
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
                    inMMSI = tokenizedline[4]
                    
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
                                track_len_m = haversine(track_obj)
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
                        track_len_m = haversine(track_obj)
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
print "Completed, " + str(preserved_count) + " segments generated, " + str(speed_discarded_count_slow) + " discarded for invalid speed (slow), " + str(speed_discarded_count_fast) + " discarded for invalid speed (fast), " + str(time_discarded_count) + " discarded for invalid time.\n"
# Print out the thresholds applied in processing.
print "Speed bounds: " + str(min_speed_bound_kts) + " < speed in knots < " + str(max_speed_bound_kts)
print "Segment (temporal) length bounds 0 < time in seconds < " + str(max_elapsed_time)
