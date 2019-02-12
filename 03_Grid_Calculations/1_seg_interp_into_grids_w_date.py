#!/usr/bin/python

# seg_interp_into_grids.py

# Import OGR/OSR
from osgeo import gdalconst
import osgeo.ogr as ogr
import osgeo.osr as osr
import rtree

import sys
import os.path
from math import sqrt

import calendar
from datetime import datetime

from osgeo import gdal
gdal.UseExceptions() 

# Simple 2d distance calculation.
def euclidian_distance(x1, y1, x2, y2):
    
    return sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1)) 

# Establish an usage message
usage_message = """
Usage: seg_interp_into_grids_w_date.py inputtrackfilename interp_field1 
interp_field2 date_field1 date_field2 inputgridfilename outputinttextfilename 
output_interp_field output_interp_date ... 
Reads the specified segment and grid (shapefile) filenames, and calculates the 
interpolation between the specified field and date along the segments into the 
grids. Assumes input layer contains simple (two point) segments with sog 
values and date values stored for the first and last points. Generates a text 
file containing the segment data plus the interpolated sog, time and grid id 
under outputinttextfilename. Developed as part of the MERIDIAN project.\n
"""

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) != 10):
    print(usage_message)
    quit()

# Copy the input/output filenames and interpolation target field from the argument vector.
in_line_file = sys.argv[1]
in_interp_field1 = sys.argv[2]
in_interp_field2 = sys.argv[3]
in_time_field1 = sys.argv[4]
in_time_field2 = sys.argv[5]
in_grid_file = sys.argv[6]
out_int_filename = sys.argv[7]
output_interp_field = sys.argv[8]
output_interp_time_field = sys.argv[9]

# Load the source line and grid layer data files.
line_data_source = ogr.Open(in_line_file, gdalconst.GA_ReadOnly)
grid_data_source = ogr.Open(in_grid_file, gdalconst.GA_ReadOnly)
line_layer = line_data_source.GetLayer()    
grid_layer = grid_data_source.GetLayer()

# Copy the spatial references from the input layers.
line_srs = line_layer.GetSpatialRef()
grid_srs = grid_layer.GetSpatialRef()

# If the spatial references don't match, display an error message and abort.
#if line_srs.ExportToWkt() != grid_srs.ExportToWkt():
if not line_srs.IsSame(grid_srs):
    print('Tracks and grids appear to have disparate SRS:\nLine:' + line_srs.ExportToWkt() + '\nGrid:' + grid_srs.ExportToWkt() + '\n')
    
    # Prompt the user to run anyway (in case of rounding error?)
    proceed_val = ''
    prompt = '> '
    while not (proceed_val == 'Y' or proceed_val == 'y' or proceed_val == 'N' or proceed_val == 'n'):
        print("Proceed anyway (Y/N)?")
        proceed_val = raw_input(prompt)

    # If the user does not indicate to proceed, exit.
    if(proceed_val == 'N' or proceed_val == 'n'):
    
        # Destroy the data sources to force close.
        line_data_source.Destroy()
        grid_data_source.Destroy()
        quit()

# Attempt to open the output file.
try:
    out_intersect_records = open(out_int_filename, 'w')
except IOError:
    print("Error opening file: " + out_int_filename + "\n")
    quit()
    
# Write a header line using the input line layer and grid layer details, for the output layer.

# Fetch the first feature from the input line layer
sample_input_feature  = line_layer.GetFeature(0)

# Iterate over the fields in the feature, adding each to the output file header
[out_intersect_records.write(sample_input_feature.GetFieldDefnRef(i).GetName() + ",") for i in range(sample_input_feature.GetFieldCount())]

# Add interpolated value and date alongside length, intersecting geometry (WKT) 
# and grid_id fields to the output layer to indicate the intersected length in grid.
out_intersect_records.write(output_interp_field + "," + output_interp_time_field + ",grd_len_km,int_wkt,grid_id\n")

# Copy the line layer definition.
line_defn = line_layer.GetLayerDefn()

###
# Establish an Index object to increase speed of access in intersection detection
index = rtree.index.Index(interleaved=False)

# Iterate over the features in the line layer (by fid), and insert the envelope for each into the index.
for line_fid in range(0, line_layer.GetFeatureCount()):
    line_feature = line_layer.GetFeature(line_fid)
    line_geometry = line_feature.GetGeometryRef()
    xmin, xmax, ymin, ymax = line_geometry.GetEnvelope()
    index.insert(line_fid, (xmin, xmax, ymin, ymax))

print("Index created.\n")
    
# Compute the total number of grid features to process:
grid_feature_count = grid_layer.GetFeatureCount()

# Iterate over the features in the grid layer (by fid), and check each against the index.
for grid_fid in range(0, grid_feature_count):
    grid_feature = grid_layer.GetFeature(grid_fid)
    grid_geometry = grid_feature.GetGeometryRef()
    xmin, xmax, ymin, ymax = grid_geometry.GetEnvelope()
    
    # For each bounding box / index intersection, perform a proper intersection calculation.
    for line_fid in list(index.intersection((xmin, xmax, ymin, ymax))):
    
        # Fetch the line 
        line_feature = line_layer.GetFeature(line_fid)
        line_geometry = line_feature.GetGeometryRef()
        
        ### If the line is contained by the grid, calculate the intersection length as 100% and the 
        # interpolation as a straight 2-point average.
        if grid_geometry.Contains(line_geometry):
        
            # Fetch the two data values from the feature.
            interp_value_1 = line_feature.GetField(in_interp_field1)
            interp_value_2 = line_feature.GetField(in_interp_field2)

            # Fetch the two dates from the feature and convert them to epoch time for interpolation.
            date_value_seconds_1 = calendar.timegm(datetime.strptime(line_feature.GetField(in_time_field1), '%Y-%m-%d %H:%M:%S').timetuple())
            date_value_seconds_2 = calendar.timegm(datetime.strptime(line_feature.GetField(in_time_field2), '%Y-%m-%d %H:%M:%S').timetuple())
            
            # Calculate the mean of the values as the output.
            interp_calc = (interp_value_1 + interp_value_2) / 2
            date_seconds_interp = (date_value_seconds_1 + date_value_seconds_2) / 2 
            
            # Generate a human-readable date from the mean date seconds
            date_value_interp = datetime.strftime(datetime.utcfromtimestamp(date_seconds_interp), '%Y-%m-%d %H:%M:%S')

            # Copy the field values from the line layer.
            for i in range(line_defn.GetFieldCount()):
                out_intersect_records.write(str(line_feature.GetField(i)) + ",")

            # Output the mean value to the output.
            out_intersect_records.write(str(interp_calc) + ",")

            # Output the interpolated date to the output.
            out_intersect_records.write(str(date_value_interp) + ",")

            # Output the whole segment length to the output layer.
            out_intersect_records.write(str(line_geometry.Length() / float(1000)) + ",")
            
            # Output an WKT of the whole segment to the output layer.
            out_intersect_records.write('"' + line_geometry.ExportToWkt() + '",')
            
            # Copy the id field value from the grid layer.
            out_intersect_records.write(str(grid_feature.GetField("Id")) + "\n")

        # If the grid intersects the line, but does not contain it, calculate an interpolated
        # value for the centre of the intersecting segment, and calculate the intersection 
        # length over the cell.
        elif grid_geometry.Intersects(line_geometry):
        
            #DEBUG print '{} intersects {}'.format(grid_fid, line_fid)
            
            # Compute the intersection result between the grid and line geometries.
            intersect_result = line_geometry.Intersection(grid_geometry)

            # Compute the centre of the intersection, using the bounding box coordinates.
            xintmin, xintmax, yintmin, yintmax = intersect_result.GetEnvelope()
            int_centre_x = (xintmax - xintmin) / 2 + xintmin
            int_centre_y = (yintmax - yintmin) / 2 + yintmin
            
            # Compute the distances from the start and end points of the segment to the centre of the intersection segment.
            start_pt = line_geometry.GetPoint(0)
            start_x = start_pt[0]
            start_y = start_pt[1]
            end_pt = line_geometry.GetPoint(line_geometry.GetPointCount() - 1)
            end_x = end_pt[0]
            end_y = end_pt[1]
            start_to_int_distance = euclidian_distance(start_x, start_y, int_centre_x, int_centre_y)
            end_to_int_distance = euclidian_distance(end_x, end_y, int_centre_x, int_centre_y)

            # Fetch the two data values from the feature.
            interp_value_1 = line_feature.GetField(in_interp_field1)
            interp_value_2 = line_feature.GetField(in_interp_field2)

            # Fetch the two dates from the feature and convert them to epoch time for interpolation.
            date_value_seconds_1 = calendar.timegm(datetime.strptime(line_feature.GetField(in_time_field1), '%Y-%m-%d %H:%M:%S').timetuple())
            date_value_seconds_2 = calendar.timegm(datetime.strptime(line_feature.GetField(in_time_field2), '%Y-%m-%d %H:%M:%S').timetuple())
            
            # Calculate a weighted interpolation from each of the two endpoint values to the centre of the intersecting 
            # segment.
            interp_calc = (interp_value_1 * end_to_int_distance + interp_value_2 * start_to_int_distance) / (start_to_int_distance + end_to_int_distance) 

            # Calculate a weighted interpolation of time from each of the two endpoint values to the centre of the intersecting 
            # segment.            
            date_seconds_interp = (date_value_seconds_1 * end_to_int_distance + date_value_seconds_2 * start_to_int_distance) / (start_to_int_distance + end_to_int_distance) 
            
            # Generate a human-readable date from the mean date seconds
            date_value_interp = datetime.strftime(datetime.utcfromtimestamp(date_seconds_interp), '%Y-%m-%d %H:%M:%S')

            # Could split lines where the geometry is multilinestring, if necessary.
            # If any geometrycollections are noted, we will need to strip non-linear 
            # components.
            
            # Determine the type of geometry in the intersection.
            intersect_geometry_type = intersect_result.GetGeometryName()
            if (intersect_result.GetGeometryType() != ogr.wkbLineString) and (intersect_result.GetGeometryType() != ogr.wkbMultiLineString):
                print('Intersection type is: ' + str(intersect_geometry_type))
                
                if intersect_result.GetGeometryType() == ogr.wkbGeometryCollection:
                    print("Geometry collection expansion: " + str(intersect_result.GetGeometryCount()) + "Geometries")
                    for i in range(0, intersect_result.GetGeometryCount()):
                        g = intersect_result.GetGeometryRef(i)
                        print("Component geometry: " + g.GetGeometryName())
                        
            # Output only line-type intersections to the output.
            else:
            
                # Copy the field values from the line layer.
                for i in range(line_defn.GetFieldCount()):
                    out_intersect_records.write(str(line_feature.GetField(i)) + ",")

                # Output the mean value to the output.
                out_intersect_records.write(str(interp_calc) + ",")

                # Output the interpolated date to the output.
                out_intersect_records.write(str(date_value_interp) + ",")

                # Copy the interected length to the output layer.
                out_intersect_records.write(str(intersect_result.Length() / float(1000)) + ",")

                # Output an WKT of the intersecting segment to the output layer.
                out_intersect_records.write('"' + intersect_result.ExportToWkt() + '",')
                
                # Copy the id field value from the grid layer.
                out_intersect_records.write(str(grid_feature.GetField("Id")) + "\n")
            
    # Provide periodic status updates.
    if grid_fid % 1000 == 0:
        print("Processing grids: " + str(grid_fid) + " of " + str(grid_feature_count))
        
# Destroy the data sources to force close.
line_data_source.Destroy()
grid_data_source.Destroy()

# Close the output file.
out_intersect_records.close()
