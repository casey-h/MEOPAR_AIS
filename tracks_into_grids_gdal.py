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
    print 'Usage: tracks_into_grids_gdal.py inputtrackfilename inputgridfilename outputintfilename ... \n Reads the specified track and grid (shapefile) filenames, and calculates the identity operation from the tracks into the grids. (output of divide_tracks.py). Generates a shapefile of the result under outputintfilename. Developed in support of the NEMES project (http://www.nemesproject.com/).\n'
    quit()

# Copy the input/output filenames from the argument vector.
in_line_file = sys.argv[1]
in_grid_file = sys.argv[2]
out_int_filename = os.path.basename(sys.argv[3])
out_int_directory = os.path.dirname(sys.argv[3])

# Load the source line and grid layer data files.
line_data_source = ogr.Open(in_line_file, gdalconst.GA_ReadOnly)
grid_data_source = ogr.Open(in_grid_file, gdalconst.GA_ReadOnly)
line_layer = line_data_source.GetLayer()    
grid_layer = grid_data_source.GetLayer()

# Copy the spatial references from the input layers.
line_srs = line_layer.GetSpatialRef()
grid_srs = grid_layer.GetSpatialRef()

# If the spatial references don't match, display an error message and abort.
if line_srs.ExportToWkt() != grid_srs.ExportToWkt():
    #print 'Cannot safely cut tracks into grids for disparate SRS:\nLine:' + to_string(line_srs) + '\nGrid:' + to_string(grid_srs) + '\n'
    print 'Cannot safely cut tracks into grids for disparate SRS:\nLine:' + line_srs.ExportToWkt() + '\nGrid:' + grid_srs.ExportToWkt() + '\n'

    # Destroy the data sources to force close.
    line_data_source.Destroy()
    grid_data_source.Destroy()
    quit()

# Set up the shapefile driver for the output layer.
driver = ogr.GetDriverByName("ESRI Shapefile")

# Create the output data source.
out_data_source = driver.CreateDataSource(out_int_directory)

# Create the output layer.
output_layer = out_data_source.CreateLayer(out_int_filename, line_srs, ogr.wkbMultiLineString)

# Line string or Multi line string? output_layer = out_data_source.CreateLayer(out_int_filename, line_srs, ogr.wkbLineString)
# Assuming multilinestring, as some intersections might be multipart

if output_layer is None:
    print "\nError encountered when creating output shapefile: " + out_int_directory + "\\" + out_int_filename + " \nAborting."
    quit()

# Copy fields from the input line layer into the output layer.

# Fetch the first feature from the input line layer
sample_input_feature  = line_layer.GetFeature(0)

# Iterate over the fields in the feature, adding each to the output layer
[output_layer.CreateField(sample_input_feature.GetFieldDefnRef(i)) for i in range(sample_input_feature.GetFieldCount())]

# Also add an Id feature to the output layer to indicate the grid.
output_layer.CreateField(ogr.FieldDefn("Grid_Id", ogr.OFTInteger))

# Copy the output layer definition.
output_defn = output_layer.GetLayerDefn()

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
            
            # Generate a new output feature.
            output_feature = ogr.Feature(output_defn)

            # Copy the field values from the line layer.
            for i in range(line_defn.GetFieldCount()):
                output_feature.SetField(line_defn.GetFieldDefn(i).GetNameRef(), line_feature.GetField(i))
                #DEBUG print "Field name: " + line_defn.GetFieldDefn(i).GetNameRef() + " value: " + str(line_feature.GetField(i))

            # Copy the id field value from the grid layer.
            output_feature.SetField("Grid_Id", grid_feature.GetField("Id"))
            
            #DEBUG print "Field name: Grid_Id value: " + str(grid_feature.GetField("Id"))
            
            # Assign the intersection geometry to the output.
            output_feature.SetGeometry(intersect_result)
            
            # Insert the output feature into the output layer, then reclaim assigned memory.
            output_layer.CreateFeature(output_feature)
            output_feature.Destroy()
            
        #Removed clause from original code snippet -- was in error 
        #else:
            #print 'Noted grid {} and line {} fids equal'.format(grid_fid, line_fid)
            
    # Provide periodic status updates.
    if grid_fid % 1000 == 0:
        print "Processing grids: " + str(grid_fid) + " of " + str(grid_feature_count)
        
# Destroy the data sources to force close.
line_data_source.Destroy()
grid_data_source.Destroy()
out_data_source.Destroy()
