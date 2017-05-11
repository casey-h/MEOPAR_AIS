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
    print 'Usage: Track_labeling_build_tracklines_gdal_BigData.py outputshapefiledirectory inputfilename [inputfilename ...] ... \n Reads the specified input filenames, presumed to correspond to csv files of waypoint data, ordered by timestamp for review in consideration of track terminus.\n'
    quit()

# Copy the output path from the argument vector.
out_line_directory = os.path.dirname(sys.argv[1])

# Set up the shapefile driver.
driver = ogr.GetDriverByName("ESRI Shapefile")

# create the spatial reference, WGS84
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)

    
# Iterate over the input files specified, parsing each.
for infile_index in range(len(sys.argv) - 2):
   
    # Attempt wildcard expansion on any input file specified.
    for in_filename in glob(sys.argv[(2 + infile_index)]):
    
        with open(in_filename,'r') as in_track_records:
        
            # Create the outfile.
            
            # Establish the output filename based on the input.
            (out_line_filename, junk) =  os.path.splitext(os.path.basename(in_filename))
            
            
            # Create the data source.
            track_data_source = driver.CreateDataSource(out_line_directory)

            # Create the track layer.
            track_layer = track_data_source.CreateLayer("track_" + out_line_filename, srs, ogr.wkbLineString)

            if track_layer is None:
                print "\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting."
                quit()

            # Define the data fields to be included in the output track layer.
            track_field_filename = ogr.FieldDefn("filename", ogr.OFTString)
            track_field_filename.SetWidth(20)
            track_layer.CreateField(track_field_filename)
        
            # Initialize flags to indicate that the current line and current point are the first in the file.
            bln_first_line = True
            bln_first_point = True
            
            # Iterate over the lines in the current file of track data.
            for line in in_track_records:
                
                # Skip over the input header line.
                if bln_first_line:
                    bln_first_line = False
                    continue
                
                # Split the incoming csv line.
                tokenizedline = line.split(',')
                
                # If the current point is the first, simply store the point value, update the first point flag and iterate.
                if (bln_first_point):
                        
                    inLat = float(tokenizedline[3])
                    inLon = float(tokenizedline[2])
                    bln_first_point = False
                    
                else:
                
                    inPrevLat = inLat
                    inPrevLon = inLon
                    inLat = float(tokenizedline[3])
                    inLon = float(tokenizedline[2])
                    
                    # Generate a feature and and populate its fields.
                    track_feature = ogr.Feature(track_layer.GetLayerDefn())
                    track_feature.SetField("filename" ,os.path.basename(in_filename))
                    
                    # Create the track geometry and assign it to the feature.
                    track_wkt = 'LINESTRING (' + str(inPrevLon) + " " + str(inPrevLat) + "," + str(inLon) + " " + str(inLat) + ")"
                        
                    track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                    track_feature.SetGeometry(track_obj)
                        
                    # Create the feature within the output layer, then reclaim assigned memory.
                    track_layer.CreateFeature(track_feature)
                    track_feature.Destroy()

            # Destroy the data source to flush features to disk.
            track_data_source.Destroy()
