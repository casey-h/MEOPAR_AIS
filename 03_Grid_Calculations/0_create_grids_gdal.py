#!/usr/bin/python

# create_grids_gdal.py

# Import OGR/OSR
from osgeo import gdalconst
from math import floor, ceil
import osgeo.ogr as ogr
import osgeo.osr as osr
import rtree

import sys
import os.path

from osgeo import gdal
gdal.UseExceptions() 

# Establish an usage string
usage_string = """Usage: create_grids_gdal.py minlon maxlon minlat maxlat projepsg gridsizeprojunits outputgridfilename ... 
    Generates a shapefile containing square cells of edge size 'gridsizemetres', under the projection projepsg, 
    covering the longitude range minlon to maxlon and latitude rangeminlat to maxlat. If the grid interval is not
    an exact fit (to be expected), the grids are centred on the ranges, and overlapping. The result is stored under the 
    provided name outputgridfilename. """

# If the wrong number of arguments is provided, display an usage message.
if (len(sys.argv) != 8):
    print(usage_string)
    quit()

# Copy and parse variables from the argument vector.
try:
    minlon = float(sys.argv[1])
    maxlon = float(sys.argv[2])
    minlat = float(sys.argv[3])
    maxlat = float(sys.argv[4])
    # Debug
    print("{}, {}, {}, {}".format(minlon, maxlon, minlat, maxlat))
except Exception:
    print("Error. Non float values specified for grid range, please specify valid latitude / longitude coordinates for the grid extent.")
    print(usage_string)
    quit()
    
try:
    projection_target_epsg = int(sys.argv[5])
    # Debug
    print("{} ".format(projection_target_epsg))
except Exception:
    print("Error. Unable to parse EPSG code as an integer value, please specify a valid epsg code for the target output projection.")
    print(usage_string)
    quit()

try:
    grid_size_metres = float(sys.argv[6])
    # Debug
    print("{} ".format(grid_size_metres))
except Exception:
    print("Error. Unable to parse grid size a float value, please specify a valid numeric value for grid edge size.")
    print(usage_string)
    quit()
    
# Copy the output filename from the argument vector.
out_filename = os.path.basename(sys.argv[7])
out_directory = os.path.dirname(sys.argv[7])

# Set up the shapefile driver.
driver = ogr.GetDriverByName("ESRI Shapefile")

# Establish the spatial reference for WGS84
wgs84_srs = osr.SpatialReference()
wgs84_srs.ImportFromEPSG(4326)

# Establish the target spatial reference from the input value.
out_srs = osr.SpatialReference()
out_srs.ImportFromEPSG(projection_target_epsg)

# Create a transform object to project WGS84 coordinates to target 
transform_wgs84_to_out = osr.CoordinateTransformation(wgs84_srs, out_srs)

# Create the output data source.
out_data_source = driver.CreateDataSource(out_directory)
if out_data_source is None:
    print("\nError encountered when opening output folder: " + out_int_directory + "\\" + " \nAborting.")
    quit()

# Create the output layer.
grid_layer = out_data_source.CreateLayer(out_filename, out_srs, ogr.wkbPolygon)
if grid_layer is None:
    print("\nError encountered when creating output shapefile: " + out_directory + "\\" + out_filename + " \nAborting.")
    quit()

# Define the data fields to be included in the output grid layer.
grid_layer.CreateField(ogr.FieldDefn("ID", ogr.OFTInteger))

# Establish the corresponding boundary coordinates from the lat/lon extents under projection.

# First build an extent poly using the lat/lon coordinates
boundary_ll_ring = ogr.Geometry(ogr.wkbLinearRing)
boundary_ll_ring.AddPoint(minlon,minlat)
boundary_ll_ring.AddPoint(minlon,maxlat)
boundary_ll_ring.AddPoint(maxlon,maxlat)
boundary_ll_ring.AddPoint(maxlon,minlat)
boundary_ll_ring.AddPoint(minlon,minlat)
boundary_ll_poly = ogr.Geometry(ogr.wkbPolygon)
boundary_ll_poly.AddGeometry(boundary_ll_ring)

# Translate the extent poly using the projection transform object from WGS84 to target
# (assuming datum shift is inconsequential / handled appropriately by OGR.)
boundary_ll_poly.Transform(transform_wgs84_to_out)

# Take the new extent from the projection result
(minprjx, maxprjx, minprjy, maxprjy) = boundary_ll_poly.GetEnvelope()

# DEBUG
print("Projected bounds: Minprjx {} Minprjy {} Maxprjx {} Maxprjy {}".format(minprjx, minprjy, maxprjx, maxprjy))

# Determine the required width of the grid extent based on the next larger interval of the grid size,
# larger than the extent in projected units / calculate the width of the grid in number of cells.
grid_cells_x = int(ceil(abs(maxprjx - minprjx)/grid_size_metres))
grid_cells_y = int(ceil(abs(maxprjy - minprjy)/grid_size_metres))

# Determine the centre of the grid / required offset at the first grid.

if (grid_cells_x % 2 == 0):
    grid_start_x = ((maxprjx + minprjx) / 2) - (grid_cells_x / 2) * grid_size_metres

else:
    grid_start_x = ((maxprjx + minprjx) / 2) - floor(grid_cells_x / 2) * grid_size_metres - grid_size_metres / 2

if (grid_cells_y % 2 == 0):
    grid_start_y = ((maxprjy + minprjy) / 2) - (grid_cells_y / 2) * grid_size_metres

else:
    grid_start_y = ((maxprjy + minprjy) / 2) - floor(grid_cells_y / 2) * grid_size_metres - grid_size_metres / 2

# Print the number of cells expected in the output.
print("Cells X:" + str(grid_cells_x))
print("Cells Y:" + str(grid_cells_y))

# Establish a grid cell id variable.
grid_cell_id = 0

# Iterate over the number of grids to be created, inserting objects into the outputs, along
# with ID numbers.
for xcellid in range(grid_cells_x):
    for ycellid in range(grid_cells_y):
        #DEBUG
        #print(xcellid)
        #print(ycellid)
        
        # Create a polygon for the current cell:
        current_cell_ring = ogr.Geometry(ogr.wkbLinearRing)
        current_cell_ring.AddPoint(grid_start_x + xcellid * grid_size_metres,grid_start_y + ycellid * grid_size_metres)
        current_cell_ring.AddPoint(grid_start_x + xcellid * grid_size_metres,grid_start_y + (ycellid + 1) * grid_size_metres)
        current_cell_ring.AddPoint(grid_start_x + (xcellid + 1) * grid_size_metres,grid_start_y + (ycellid + 1) * grid_size_metres)
        current_cell_ring.AddPoint(grid_start_x + (xcellid + 1) * grid_size_metres,grid_start_y + ycellid * grid_size_metres)
        current_cell_ring.AddPoint(grid_start_x + xcellid * grid_size_metres,grid_start_y + ycellid * grid_size_metres)
        cell_poly = ogr.Geometry(ogr.wkbPolygon)
        cell_poly.AddGeometry(current_cell_ring)

        grid_cell_feature = ogr.Feature(grid_layer.GetLayerDefn())
        grid_cell_feature.SetField("ID",grid_cell_id)
        grid_cell_feature.SetGeometry(cell_poly)
        
        # Increment the grid cell id.
        grid_cell_id += 1
        
        # Create the feature within the output layer, then reclaim assigned memory.
        grid_layer.CreateFeature(grid_cell_feature)
        grid_cell_feature.Destroy()
        
# Destroy the data sources to force close.
out_data_source.Destroy()
