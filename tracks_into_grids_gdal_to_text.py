#!/usr/bin/python

# tracks_into_grids_gdal.py

# Import OGR/OSR
from osgeo import gdalconst
import osgeo.ogr as ogr
import osgeo.osr as osr
import rtree

import sys
import os.path

from osgeo import gdal
gdal.UseExceptions() 

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) < 4):
    print 'Usage: tracks_into_grids_gdal.py inputtrackfilename inputgridfilename outputinttextfilename ... \n Reads the specified track and grid (shapefile) filenames, and calculates the identity operation from the tracks into the grids. (output of divide_tracks.py). Generates a text file containing the segment data plus the intersection length and grid id under outputinttextfilename. Developed in support of the NEMES project (http://www.nemesproject.com/).\n'
    quit()

# Copy the input/output filenames from the argument vector.
in_line_file = sys.argv[1]
in_grid_file = sys.argv[2]
out_int_filename = sys.argv[3]

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
    print 'Tracks and grids appear to have disparate SRS:\nLine:' + line_srs.ExportToWkt() + '\nGrid:' + grid_srs.ExportToWkt() + '\n'
    
    # Prompt the user to run anyway (in case of rounding error?)
    proceed_val = ''
    prompt = '> '
    while not (proceed_val == 'Y' or proceed_val == 'y' or proceed_val == 'N' or proceed_val == 'n'):
        print "Proceed anyway (Y/N)?"
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
    print "Error opening file: " + out_int_filename + "\n"
    quit()
    
# Write a header line using the input line layer and grid layer details, for the output layer.

# Fetch the first feature from the input line layer
sample_input_feature  = line_layer.GetFeature(0)

# Iterate over the fields in the feature, adding each to the output file header
[out_intersect_records.write(sample_input_feature.GetFieldDefnRef(i).GetName() + ",") for i in range(sample_input_feature.GetFieldCount())]

# Add length and grid_id fields to the output layer to indicate the intersected length in grid.
out_intersect_records.write("grd_len_km,grid_id\n")

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

print "Index created.\n"
    
# Compute the total number of grid features to process:
grid_feature_count = grid_layer.GetFeatureCount()

# Iterate over the features in the grid layer (by fid), and check each against the index.
for grid_fid in range(0, grid_feature_count):
    grid_feature = grid_layer.GetFeature(grid_fid)
    grid_geometry = grid_feature.GetGeometryRef()
    xmin, xmax, ymin, ymax = grid_geometry.GetEnvelope()
    
    # For each bounding box / index intersection, perform a proper intersection calculation.
    for line_fid in list(index.intersection((xmin, xmax, ymin, ymax))):
    
        # Removed clause to test between fid values, which was present in initial code snippet
        # erroneously missed collisions between fids common to both layers
        # if line_fid != grid_fid:
    
        # Fetch the line 
        line_feature = line_layer.GetFeature(line_fid)
        line_geometry = line_feature.GetGeometryRef()
        
        # Testing the intersect_result <> None gives false (trivial?) 
        # intersections, while testing via .Intersects() appears to give 
        # the expected result.
        if grid_geometry.Intersects(line_geometry):
        
            #DEBUG print '{} intersects {}'.format(grid_fid, line_fid)
            
            # Compute the intersection result between the grid and line geometries.
            intersect_result = line_geometry.Intersection(grid_geometry)
            
            # Could split lines where the geometry is multilinestring, if necessary.
            # If any geometrycollections are noted, we will need to strip non-linear 
            # components.
            
            # Determine the type of geometry in the intersection.
            intersect_geometry_type = intersect_result.GetGeometryName()
            if (intersect_result.GetGeometryType() <> ogr.wkbLineString) and (intersect_result.GetGeometryType() <> ogr.wkbMultiLineString):
                print 'Intersection type is: ' + str(intersect_geometry_type)
                
                if intersect_result.GetGeometryType() == ogr.wkbGeometryCollection:
                    print "Geometry collection expansion: " + str(intersect_result.GetGeometryCount()) + "Geometries"
                    for i in range(0, intersect_result.GetGeometryCount()):
                        g = intersect_result.GetGeometryRef(i)
                        print "Component geometry: " + g.GetGeometryName()
            

            # Copy the field values from the line layer.
            for i in range(line_defn.GetFieldCount()):
                out_intersect_records.write(str(line_feature.GetField(i)) + ",")

            # Copy the interected length to the output layer.
            out_intersect_records.write(str(intersect_result.Length() / float(1000)) + ",")
            
            # Copy the id field value from the grid layer.
            out_intersect_records.write(str(grid_feature.GetField("Id")) + "\n")
            
    # Provide periodic status updates.
    if grid_fid % 1000 == 0:
        print "Processing grids: " + str(grid_fid) + " of " + str(grid_feature_count)
        
# Destroy the data sources to force close.
line_data_source.Destroy()
grid_data_source.Destroy()

# Close the output file.
out_intersect_records.close()
