#!/usr/bin/python

# Import OGR/OSR
import osgeo.ogr as ogr
import osgeo.osr as osr

import sys
import os.path
from glob import glob

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
    print 'Usage: Track_labeling_build_points_gdal_BigData.py outputshapefiledirectory inputfilename [inputfilename ...] ... \n Reads the specified input filenames, presumed to correspond to csv files of waypoint data, ordered by timestamp for review in consideration of track terminus.\n'
    quit()

# Copy the output path from the argument vector.
out_point_directory = os.path.dirname(sys.argv[1])

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
            (out_point_filename, junk) =  os.path.splitext(os.path.basename(in_filename))
            
            # Create the data source.
            point_data_source = driver.CreateDataSource(out_point_directory)

            # Create the point layer.
            point_layer = point_data_source.CreateLayer("point_" + out_point_filename, srs, ogr.wkbPoint)

            if point_layer is None:
                print "\nError encountered when creating output point shapefile: " + out_point_directory + "\\" + out_point_filename + " \nAborting."
                quit()

            # Define the data fields to be included in the output point layer.
            
            # MMSI	Lon	Lat	Time	ttn	dtn	vc	vel	angl	div	cl	port	brk, manual (flag field)
            point_layer.CreateField(ogr.FieldDefn("MMSI", ogr.OFTInteger))
            
            point_field_lon = ogr.FieldDefn("Lon", ogr.OFTString)
            point_field_lon.SetWidth(25)
            point_layer.CreateField(point_field_lon)
            
            point_field_lat = ogr.FieldDefn("Lat", ogr.OFTString)
            point_field_lat.SetWidth(25)
            point_layer.CreateField(point_field_lat)
            
            point_field_time = ogr.FieldDefn("Time", ogr.OFTString)
            point_field_time.SetWidth(22)
            point_layer.CreateField(point_field_time)

            point_field_ttn = ogr.FieldDefn("ttn", ogr.OFTString)
            point_field_ttn.SetWidth(25)
            point_layer.CreateField(point_field_ttn)
            
            point_field_dtn = ogr.FieldDefn("dtn", ogr.OFTString)
            point_field_dtn.SetWidth(25)
            point_layer.CreateField(point_field_dtn)
            
            point_field_vc = ogr.FieldDefn("vc", ogr.OFTString)
            point_field_vc.SetWidth(25)
            point_layer.CreateField(point_field_vc)
            
            point_field_vel = ogr.FieldDefn("vel", ogr.OFTString)
            point_field_vel.SetWidth(25)
            point_layer.CreateField(point_field_vel)
            
            point_field_angl = ogr.FieldDefn("angl", ogr.OFTString)
            point_field_angl.SetWidth(25)
            point_layer.CreateField(point_field_angl)
            
            point_field_div = ogr.FieldDefn("div", ogr.OFTString)
            point_field_div.SetWidth(6)
            point_layer.CreateField(point_field_div)
            
            point_layer.CreateField(ogr.FieldDefn("cl", ogr.OFTInteger))

            point_field_port = ogr.FieldDefn("port", ogr.OFTString)
            point_field_port.SetWidth(6)
            point_layer.CreateField(point_field_port)
            
            point_field_brk = ogr.FieldDefn("brk", ogr.OFTString)
            point_field_brk.SetWidth(6)
            point_layer.CreateField(point_field_brk)
            
            point_layer.CreateField(ogr.FieldDefn("manual", ogr.OFTInteger))
            
            # Initialize flags to indicate that the current line and current point are the first in the file.
            bln_first_line = True
            
            # Iterate over the lines in the current file of track data.
            for line in in_track_records:
                
                # Skip over the input header line.
                if bln_first_line:
                    bln_first_line = False
                    continue
                
                # Split the incoming csv line.
                tokenizedline = line.split(',')

                inLon = float(tokenizedline[2])                
                inLat = float(tokenizedline[3])
                
                # Generate a feature and and populate its fields.
                point_feature = ogr.Feature(point_layer.GetLayerDefn())
                
                ####
                point_feature.SetField("MMSI", tokenizedline[1])
                point_feature.SetField("Lon", tokenizedline[2])
                point_feature.SetField("Lat", tokenizedline[3])
                point_feature.SetField("Time", tokenizedline[4])
                point_feature.SetField("ttn", tokenizedline[5])
                point_feature.SetField("dtn", tokenizedline[6])
                point_feature.SetField("vc", tokenizedline[7])
                point_feature.SetField("vel", tokenizedline[8])
                point_feature.SetField("angl", tokenizedline[9])
                point_feature.SetField("div", tokenizedline[10])
                point_feature.SetField("cl", tokenizedline[11])
                point_feature.SetField("port", tokenizedline[12])
                point_feature.SetField("brk", tokenizedline[13])
                point_feature.SetField("manual", 0)
                ####
                
                
                # Create the point geometry and assign it to the feature.
                point_wkt = "POINT(%f %f)" % (inLon, inLat)
                point_obj = ogr.CreateGeometryFromWkt(point_wkt)
                point_feature.SetGeometry(point_obj)
                    
                # Create the feature within the output layer, then reclaim assigned memory.
                point_layer.CreateFeature(point_feature)
                point_feature.Destroy()

            # Destroy the data source to flush features to disk.
            point_data_source.Destroy()
