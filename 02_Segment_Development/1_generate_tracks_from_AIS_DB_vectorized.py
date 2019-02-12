#!/usr/bin/python
# Split pre-parsed exactEarth position-referenced AIS data (Postgres Export) 
# into basic movement data files on message type group. Requires GDAL / OGR/OSR, and
# progressbar2 packages for Python.

# Modifications
# 2018-06-26: Removed position accuracy from processed fields.

# 2018-09-20: Adding CSV input file processing, converting argument handling to argparser.

# Disable numpy warnings about type - change due to mismatched build of numpy vs python
#https://stackoverflow.com/questions/40845304/runtimewarning-numpy-dtype-size-changed-may-indicate-binary-incompatibility
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
#https://stackoverflow.com/questions/40845304/runtimewarning-numpy-dtype-size-changed-may-indicate-binary-incompatibility
# Disable numpy warnings about type - change due to mismatched build of numpy vs python

from glob import glob
import sys
import os
import time
import calendar
from datetime import datetime
import progressbar

from math import radians, cos, sin, asin, sqrt, atan2, pi

# Import the path separator.
from os import sep

# Import OGR/OSR
import osgeo.ogr as ogr
import osgeo.osr as osr

# Import database, dataframe, numpy support.
import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd
import numpy as np
from numpy.lib.recfunctions import append_fields

# Import compress for subsetting lists using boolean arrays
from itertools import compress

# Import argument parser
import argparse

# Import parser for dates in csv files for conversion to dataframe.
import dateutil.parser

#########################################################
# Constants

# Input AIS data spatial reference (EPSG#) - WGS84
inputEPSG = 4326

# Minimum inferred speed boundary (for point-to-point tracks, GIS file creation) (knots, 2016-04-01; modified -> 0.5 2018-10-22)
min_speed_bound_kts = 0.5

#Maximum inferred speed boundary (for point-to-point tracks, GIS file creation) (knots 2016-04-01)
max_speed_bound_kts = 86.3930885411603
#########################################################

"""
from itertools import tee, izip

# Function to take elements from an iterable by pairs sequentially. (source: https://stackoverflow.com/questions/5389507/iterating-over-every-two-elements-in-a-list)
def pairwise_sequential(iterable):
    "s -> (s0, s1), (s2, s3), (s4, s5), ..."
    a = iter(iterable)
    return izip(a, a)


# Function to take elements from an iterable by pairs overlapping. (source: https://stackoverflow.com/questions/23151246/iterrows-pandas-get-next-rows-value/23155098#23155098)
def pairwise_overlapping(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)
"""

# Function (is_number) to determine if a string represents a number (specifically, an mmsi, code from: http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python).
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
# End function (is_number)


# Function (haversine_coords_km) to calculate approximate distance between point pair in km.
def haversine_coords_km(lon1, lat1, lon2, lat2):

    #Calculate the great circle distance between two points 
    #on the earth (specified in decimal degrees). Adjusted to 
    #return output on the range [0, 360)
    
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
# End function (haversine_coords)
    
# Function (haversine_geom_m) to calculate approximate distance along segment geometry in m.
def haversine_geom_m(inGeometry):

    #DEBUG
    if (inGeometry.GetPointCount() > 2):
        # Adjust to print function / python3 CH 20180107 (Add parens)
        print("Too many points in haversine?")
    
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
# End function (haversine_geom_m)
    
# Function (bearing) to calculate bearing travelled from one set of coordinates to reach a second.
# Drawn from calculations at: http://www.movable-type.co.uk/scripts/latlong.html
def bearing(lon1, lat1, lon2, lat2):

    # convert decimal degrees to radians 
    lambda_one, phi_one, lambda_two, phi_two = map(radians, [lon1, lat1, lon2, lat2])
    
    # Calculate the difference in x / y movement.
    y = sin(lambda_two-lambda_one) * cos(phi_two);
    x = cos(phi_one)*sin(phi_two) - sin(phi_one)*cos(phi_two)*cos(lambda_two-lambda_one);
        
    # Calculate the initial bearing.
    bearing = atan2(y, x) * 180 / pi
    
    # Convert the bearing to the range [0, 360) for values less than 0.
    if bearing < 0:
        bearing = (bearing + 360) % 360
        
    return bearing
# End function (bearing)

# Function (parseSOG) - Function to interpret possible SOG string values as a numeric value.
def parseSOG(inSOGStr):
    if ((inSOGStr == "n/a") or (inSOGStr == "None") or (inSOGStr == "")):
        return float(102.3)
    elif inSOGStr == "fast":
        return float(102.2)
    else:
        return float(inSOGStr)
# End function (parseSOG)

# Function (parseCOG) - Function to interpret possible COG string values as a numeric value.
def parseCOG(inCOGStr):
    if ((inCOGStr == "n/a") or (inCOGStr == "None") or (inCOGStr == "")):
        return float(360.0)
    else:
        return float(inCOGStr)
# End Function (parseCOG)

# Function (parseTrHdg) - Function to interpret possible True Heading string values as a numeric value.
def parseTrHdg(inTrHdgStr):
    if ((inTrHdgStr == "n/a") or (inTrHdgStr == "None") or (inTrHdgStr == "")):
        return float(511.0)
    else:
        return float(inTrHdgStr)
# End Function (parseTrHdg)

# Function (extract_pgpass_conn_string) - Extracts the first line of text from a pgpass style
# input file and builds a DSN style connection string from it.
def extract_pgpass_conn_string(cmd_connect_string_filename):
    
    # Try to open the indicated file and split out a single line of connection string detail.
    try:
 
        conn_string_file = open(cmd_connect_string_filename, 'r')
        in_raw_conn_string = conn_string_file.readline()

        # Pgpass files are tokenized on colon.
        tokenized_conn_string = in_raw_conn_string.split(':')

        # Require that all 5 elements are present, form and return a string if so, otherwise 
        # close the input and exit.
        if len(tokenized_conn_string) == 5:

            return "host={} port={} dbname={} user={} password={}".format(tokenized_conn_string[0], tokenized_conn_string[1], tokenized_conn_string[2], tokenized_conn_string[3], tokenized_conn_string[4])

        else:

            conn_string_file.close()
            return ""
            
    except IOError:
        
        return ""
        
    conn_string_file.close()
# End Function (extract_pgpass_conn_string)


# Function csv_date_parse - Helper function to parse datetime values as expected for further processing.
def csv_date_parse(in_str_datetime):

    parsed_date = dateutil.parser.parse(in_str_datetime, ignoretz=True)
    return parsed_date

# Function (generate_dataframe_csv) - Function to instantiate a dataframe for processing, using 
# an input csv.
def generate_dataframe_csv(in_csvfile):

    # Load the csv file to a dataframe
    loaded_dataframe = pd.read_csv(in_csvfile,header=0,index_col=False,usecols=['time','message_id','mmsi','navigational_status','sog','cog','heading','latitude','longitude'],na_values='',memory_map=True, parse_dates=['time'],date_parser=csv_date_parse)
    loaded_dataframe = loaded_dataframe[['time','message_id','mmsi','navigational_status','sog','cog','heading','latitude','longitude']]

    # Sort the dataframe on mmsi, time, message_id, latitude, longitude, cog, sog, heading (for repeatability)
    loaded_dataframe.sort_values(by=['mmsi','time','message_id','latitude','longitude','cog','sog','heading'],inplace=True)

    # Reset index to match new sorting 
    loaded_dataframe.reset_index(drop=True)

    return loaded_dataframe

# Function (generate_dataframe_pgdb) - Function to instantiate a dataframe for processing, based on 
# Postgres DB connection.
def generate_dataframe_pgdb(in_connect_string, in_tablename):
    
    # Establish a connection to the database and query the records from the indicated table.
    connection = pg.connect(in_connect_string)

    # Select the data records in sorted order on mmsi, time, message_id, latitude, longitude, cog, sog, heading (for repeatability)
    dataframe = psql.read_sql("SELECT time, message_id, mmsi, navigational_status, sog, cog, heading, latitude, longitude FROM " + in_tablename + " order by mmsi, time, message_id, latitude, longitude, cog, sog, heading", connection, parse_dates='time')

    return dataframe
    
# End Function (generate_dataframe_pgdb)

# Function (calc_time) - A function to calculate elapsed time
# between a pair of timestamps from AIS point records.
def calc_time(time_AIS_array_val_1, time_AIS_array_val_2):

    # Calculate the difference in time, convert to units of s.
    return (time_AIS_array_val_2 - time_AIS_array_val_1) / np.timedelta64(1, 's')
    
# End Function (calc_time)

# Function (create_segment_WKT) - Build a WKT line representation from two coordinate pairs.
# Adjusted for single input re: map 
def create_segment_WKT(pt_AIS_array_lon_1, pt_AIS_array_lat_1, pt_AIS_array_lon_2, pt_AIS_array_lat_2):
#def create_segment_WKT(pt_AIS_array_in):

    return 'LINESTRING (' + str(pt_AIS_array_lon_1) + " " + str(pt_AIS_array_lat_1) + ", " + str(pt_AIS_array_lon_2) + " " + str(pt_AIS_array_lat_2) + ")"
    #return 'LINESTRING (' + str(pt_AIS_array_in[0]) + " " + str(pt_AIS_array_in[1]) + ", " + str(pt_AIS_array_in[2]) + " " + str(pt_AIS_array_in[3]) + ")"

# End Function (create_segment_WKT)

# Function (create_threshold_traj_WKT) - Build a WKT line segment representation from an array of coordinate pairs.
# Adjusted for single input re: map 
def create_threshold_traj_WKT(pt_AIS_array_lon, pt_AIS_array_lat):

    return 'LINESTRING (' + ",".join(map(lambda x,y: str(x) + " " + str(y), pt_AIS_array_lon, pt_AIS_array_lat)) + ')'

# End Function (create_threshold_traj_WKT)

# Function (segment_pairwise) - Creates a pair of numpy arrays from an input dataframe
# of AIS data, having a 'time' field, and a separate time_threshold value.
def segment_pairwise(in_AIS_dataframe, time_threshold):

    # Create lists to hold the output data for each of the segment and discard dataframes.
    out_segment_AIS_items = []
    out_segment_AIS_items_segidx = []
    out_discarded_AIS_items = []
    out_discarded_AIS_items_reason = []
    
    # If the dataframe only contains a single point, output it as
    # stranded.
    if(in_AIS_dataframe.shape[0] == 1):
        
        out_discarded_AIS_items.append(np.asarray(in_AIS_dataframe.to_records(),[('index', '<i8'), ('time', '<M8[ns]'), ('message_id', '<i8'), ('mmsi', '<i8'), ('navigational_status', '<i8'), ('sog', '<f8'), ('cog', '<f8'), ('heading', '<f8'), ('latitude', '<f8'), ('longitude', '<f8')])[0])
        out_discarded_AIS_items = append_fields(out_discarded_AIS_items, 'reason', np.asarray(["Stranded single point"]))

        return(out_segment_AIS_items, out_discarded_AIS_items)

    # Establish a segment counter.
    segment_counter = 0

    # Translate the incoming dataframe to a Numpy NDarray of predictable dtype
    in_AIS_data_array = np.asarray(in_AIS_dataframe.to_records(),[('index', '<i8'), ('time', '<M8[ns]'), ('message_id', '<i8'), ('mmsi', '<i8'), ('navigational_status', '<i8'), ('sog', '<f8'), ('cog', '<f8'), ('heading', '<f8'), ('latitude', '<f8'), ('longitude', '<f8')])
    
    # Identify points with bad coordinates to be dropped.
    invalid_lat = np.array(abs(in_AIS_data_array['latitude']) > 90, dtype=bool)
    invalid_lon = np.array(abs(in_AIS_data_array['longitude']) > 180, dtype=bool)
    invalid_coord = np.where(np.logical_or(invalid_lat, invalid_lon))
    
    # Copy invalid coordinates to output.
    invalid_records = in_AIS_data_array[invalid_coord]

    # Copy records w/ invalid coordinates to discarded items.
    if (invalid_records.shape[0] > 0):
        out_discarded_AIS_items.extend(invalid_records)
        out_discarded_AIS_items_reason.extend(["Out of range"] * invalid_records.shape[0])
    
    # Delete invalid coordinates from array.
    in_AIS_data_array = np.delete(in_AIS_data_array,invalid_coord,0)
    
    # Create slices of [1,n-1] and [2,n] elements for pairwise comparison.
    first_pts_AIS_array = in_AIS_data_array[0:-1]
    second_pts_AIS_array = in_AIS_data_array[1:]

    # Calculate elapsed times between the two arrays.
    elapsed_times = map(calc_time, first_pts_AIS_array['time'], second_pts_AIS_array['time'])

    # Calculate duplicate positions in points between the two arrays.
    dup_positions = np.logical_and(np.equal(first_pts_AIS_array['longitude'], second_pts_AIS_array['longitude']), np.equal(first_pts_AIS_array['latitude'], second_pts_AIS_array['latitude']))

    # Identify duplicates on coordinate, time through the two computed arrays
    dup_indices = np.array(np.logical_or((np.equal(elapsed_times, 0)), (dup_positions)), dtype=bool)

    # Select duplicates and add to discarded output (both)
    duplicate_first_points = first_pts_AIS_array[dup_indices]
    duplicate_second_points = second_pts_AIS_array[dup_indices]

    # Copy records from duplicate outputs to discarded items.
    if (duplicate_first_points.shape[0] > 0):
        out_discarded_AIS_items.extend(duplicate_first_points)
        out_discarded_AIS_items_reason.extend(["Duplicate point/time (pt1's)"] * duplicate_first_points.shape[0])
        
    if (duplicate_second_points.shape[0] > 0):
        out_discarded_AIS_items.extend(duplicate_second_points)
        out_discarded_AIS_items_reason.extend(["Duplicate point/time (pt2's)"] * duplicate_second_points.shape[0])

    # Delete out duplicates.
    first_pts_AIS_array_nodups = first_pts_AIS_array[np.logical_not(dup_indices)]
    second_pts_AIS_array_nodups = second_pts_AIS_array[np.logical_not(dup_indices)]
    
    # Re-calculate elapsed times between the two arrays.
    elapsed_times = map(calc_time, first_pts_AIS_array_nodups['time'], second_pts_AIS_array_nodups['time'])

    # Identify segments outside thresholds on time. 
    out_of_time_range = np.array(np.greater(elapsed_times,time_threshold), dtype=bool)

    # Select outside time threshold and add to discarded output (both).
    out_of_range_first_points = first_pts_AIS_array_nodups[out_of_time_range]
    out_of_range_second_points = second_pts_AIS_array_nodups[out_of_time_range]

    # Copy records from threshold outputs to discarded items.
    if (out_of_range_first_points.shape[0] > 0):
    
        out_discarded_AIS_items.extend(out_of_range_first_points)
        out_discarded_AIS_items_reason.extend(["Time threshold exceeded (pt1's)"] * out_of_range_first_points.shape[0])

    if (out_of_range_second_points.shape[0] > 0):
    
        out_discarded_AIS_items.extend(out_of_range_second_points)
        out_discarded_AIS_items_reason.extend(["Time threshold exceeded (pt2's)"] * out_of_range_second_points.shape[0])

    # Delete thresholded points.
    first_pts_AIS_array_thresh = first_pts_AIS_array_nodups[np.logical_not(out_of_time_range)]
    second_pts_AIS_array_thresh = second_pts_AIS_array_nodups[np.logical_not(out_of_time_range)]

    # Establish a counter into the output segments.
    segment_counter = 0

    # If there are records remaining, iterate pairwise over the values, inserting them into output segments.
    if ((len(first_pts_AIS_array_thresh) > 0) and (len(second_pts_AIS_array_thresh) > 0)):

        
        # Iterate pairwise through first/second datapoint arrays.
        for (pre_row, curr_row) in zip(np.nditer(first_pts_AIS_array_thresh), np.nditer(second_pts_AIS_array_thresh)):

            # Append the first and second points in sequence, with a common segment counter.
            out_segment_AIS_items.append(pre_row)
            out_segment_AIS_items_segidx.append(segment_counter)
            out_segment_AIS_items.append(curr_row)
            out_segment_AIS_items_segidx.append(segment_counter)

            # Increment the segment counter
            segment_counter += 1

    # Merge the data and labels from the lists of discarded and preserved records into
    # the output arrays. If no records exist retain empty arrays.
    if(len(out_segment_AIS_items) > 0):

        out_segment_AIS_items = append_fields(np.asarray(out_segment_AIS_items), 'segidx', np.asarray(out_segment_AIS_items_segidx))
        
    if(len(out_discarded_AIS_items) > 0):
        
        out_discarded_AIS_items = append_fields(np.asarray(out_discarded_AIS_items), 'reason', np.asarray(out_discarded_AIS_items_reason))

    return (out_segment_AIS_items, out_discarded_AIS_items)

# End Function (segment_pairwise)

# Function (split_pre_tracks) - Split raw AIS message data into separate files by message type, and then 
# by mmsi.
def split_pre_tracks(dataframe, split_file_directory):
    
    #Establish a progressbar for iterating over incoming records
    in_records_bar = progressbar.ProgressBar()
    
    # Iterate over the incoming dataframe records, outputting files on the basis of mmsi,
    #  while tracking progress with a progressbar.
    for datarow in in_records_bar(dataframe.itertuples()):
         
        try:
            outfile = open(split_file_directory  + sep + str(datarow.mmsi) + ".txt", 'a')

        except IOError:
            # Adjust to print function / python3 CH 20180107 (Add parens)
            print("Error opening file: " + split_file_directory + sep + str(datarow.mmsi) + "\n")
            quit()
        
        lineout = "{},{},{},{},{},{},{},{},{}\n".format(datarow.time, datarow.message_id, datarow.mmsi, datarow.navigational_status, datarow.sog, datarow.cog, datarow.heading, datarow.latitude, datarow.longitude)
        outfile.write(lineout)
        outfile.close()

# End Function (split_pre_tracks)

# Function (generate_short_segments) - Split mmsi-separated text position data into short segments
# based on identified thresholds.
def generate_short_segments(in_dataframe, segment_split_directory, segment_other_split_directory, max_elapsed_time):
    
    # Select the mmsi records in sorted order on mmsi.
    dataframe_mmsis = in_dataframe['mmsi'].unique()

    #Establish a progressbar for iterating over incoming records
    in_records_bar = progressbar.ProgressBar()

    # Establish an overall array to hold all segmented records.
    all_generated_segments_array = None

    # Establish values to track the number of points discarded as each of
    # duplicate points, time threshold exceeded and stranded points
    dup_discards = 0
    time_discards = 0
    stranded_discards = 0

    # Establish a value to track the total number of input points.
    total_points = 0
    
    # Iterate over the incoming dataframe records, processing each vessel / mmsi in
    # sequence while tracking progress with a progressbar.
    with progressbar.ProgressBar(max_value=dataframe_mmsis.size) as in_data_bar:
        mmsi_iterator = np.nditer(dataframe_mmsis, flags=['f_index'])
        while not mmsi_iterator.finished:

            # Copy the current mmsi value from the iterator.
            mmsidatarow = mmsi_iterator[0]

            # Use the current mmsi to fetch a second dataframe holding the point data 
            # for the current vessel, sorted on date / message_id / lat / lon.
            dataframe_points = in_dataframe.query('mmsi == @mmsidatarow')
# DEBUG Re: sorting            dataframe_points.sort_values(by=['time', 'message_id', 'latitude', 'longitude'])
            dataframe_points.sort_values(by=['time', 'message_id', 'latitude', 'longitude', 'cog', 'sog', 'heading'])
            
            # Update the number of total input points
            total_points += dataframe_points.shape[0]

            # Request segmentation of the point data.
            (segments_array, discards_array) = segment_pairwise(dataframe_points, max_elapsed_time)

            ### DEBUG - Alternate printout of segment data
            if (len(segments_array) > 0):
                
                try:
                    out_vessel_records = open(segment_split_directory + sep + str(mmsidatarow) + ".txt", 'w')
                
                except IOError:
                    # Adjust to print function / python3 CH 20180107 (Add parens)
                    print("Error opening file: " + segment_split_directory + sep + str(mmsidatarow) + ".txt" + "\n")
                    quit()

                for pointdatarow in np.nditer(segments_array): 
                    
                    lineout = "{},{},{},{},{},{},{},{:.0f},{},{}\n".format(pointdatarow['segidx'], pointdatarow['time'], pointdatarow['message_id'], pointdatarow['mmsi'], pointdatarow['navigational_status'], pointdatarow['sog'], pointdatarow['cog'], pointdatarow['heading'], pointdatarow['latitude'], pointdatarow['longitude'])
                    out_vessel_records.write(lineout)

                # Close the output file.
                out_vessel_records.close()
            
            # Output the discarded points to the invalid datafile.
            if (len(discards_array) > 0):
                
                try:
                    # Modified - mmsidatarow now mmsi only 20180920 out_invalid_records = open(segment_other_split_directory + sep + "invalid_" + str(mmsidatarow.mmsi) + ".txt", 'w')
                    out_invalid_records = open(segment_other_split_directory + sep + "invalid_" + str(mmsidatarow) + ".txt", 'w')
                
                except IOError:
                    # Adjust to print function / python3 CH 20180107 (Add parens)
                    # Modified - mmsidatarow now mmsi only 20180920 print("Error opening file: " + segment_other_split_directory + sep + "invalid_" + str(mmsidatarow.mmsi) + ".txt" + "\n")
                    print("Error opening file: " + segment_other_split_directory + sep + "invalid_" + str(mmsidatarow) + ".txt" + "\n")
                    quit()

                for pointdatarow in np.nditer(discards_array): 
                                        
                    lineout = "{},{},{},{},{},{},{},{},{},{}\n".format(pointdatarow['time'], pointdatarow['message_id'], pointdatarow['mmsi'], pointdatarow['navigational_status'], pointdatarow['sog'], pointdatarow['cog'], pointdatarow['heading'], pointdatarow['latitude'], pointdatarow['longitude'], pointdatarow['reason'])
                    out_invalid_records.write(lineout)

                # Close the output file.
                out_invalid_records.close()
                
                # Calculate the number of various discard types from the records returned.
                dup_discards = dup_discards + len(np.where(discards_array['reason'] == "Duplicate point/time (pt1's)")[0])
                time_discards = time_discards + len(np.where(discards_array['reason'] == "Time threshold exceeded (pt1's)")[0])
                stranded_discards = stranded_discards + len(np.where(discards_array['reason'] == "Stranded single point")[0])
                
            # If there are any current segment records, incorporate them into the overall list.
            if(all_generated_segments_array is None or (len(all_generated_segments_array) == 0)):

                all_generated_segments_array = segments_array

            elif (len(segments_array) > 0):
                
                all_generated_segments_array = np.concatenate((all_generated_segments_array,segments_array))

            # Update the Progressbar using the index from the iterator.
            in_data_bar.update(mmsi_iterator.index)

            # Increment the iterator.
            mmsi_iterator.iternext()

    # Print a message indicating the type and number of discards in the segmentation.
    print("\nShort segmentation complete:\n\t {} input points, \n\t {} stranded points discarded, \n\t {} segments discarded as duplicate start/end and \n\t {} segments discarded as time threshold exceeded ({} seconds).\n".format(total_points, stranded_discards,dup_discards,time_discards,max_elapsed_time))

    # Return the segmented track data.
    return all_generated_segments_array

# Function write_track_record - Function to write out a single short track / segment 
# record from two AIS points. 
def write_short_track_record(track_layer_val, first_pts_AIS, second_pts_AIS, track_obj, elapsed_seconds_val, track_len_m_val, speed_knots_val):
    
    # Generate a feature and and populate its fields.
    track_feature = ogr.Feature(track_layer_val.GetLayerDefn())
    track_feature.SetField("TrackID" , first_pts_AIS['segidx'])
    track_feature.SetField("mmsi" , first_pts_AIS['mmsi'])
    track_feature.SetField("elp_sec" , elapsed_seconds_val)
    # The date conversion below is madness, but apparrently the way to go: https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64 
    track_feature.SetField("st_date" , pd.to_datetime(str(first_pts_AIS['time'])).strftime("%Y-%m-%d %H:%M:%S"))
    track_feature.SetField("en_date" , pd.to_datetime(str(second_pts_AIS['time'])).strftime("%Y-%m-%d %H:%M:%S"))
    track_feature.SetField("seg_len_km" , track_len_m_val / float(1000))
    track_feature.SetField("distspdkts" , speed_knots_val)
    track_feature.SetField("sogpt1",first_pts_AIS['sog'])
    track_feature.SetField("sogpt2",second_pts_AIS['sog'])
    track_feature.SetField("avg_sog",(first_pts_AIS['sog']+second_pts_AIS['sog'])/2)
    
    # Assign the geometry to the feature.
    track_feature.SetGeometry(track_obj)
    
    # Create the feature within the output layer, then reclaim assigned memory.
    track_layer_val.CreateFeature(track_feature)
    track_feature.Destroy()

# End Function write_track_record

# Function generate_short_GIS - Write out a GIS representation of the output
# of a short / point-to-point segmentation.
def generate_short_GIS(segment_split_array, gis_directory, out_filename_prefix, max_elapsed_time, outputEPSG):
    
    # Generate a line file output filename.
    out_line_filename = os.path.basename(gis_directory + sep + out_filename_prefix + "_lines")
    out_line_directory = os.path.dirname(gis_directory + sep + out_filename_prefix + "_lines")

    # Set up the shapefile driver.
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Create the data source.
    track_data_source = driver.CreateDataSource(out_line_directory)

    # create the spatial reference, WGS84
    in_srs = osr.SpatialReference()
    in_srs.ImportFromEPSG(inputEPSG)

    # Create the output spatial reference, BC Albers
    # Presuming effective equality between NAD83 / WGS84 over function scope.
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(outputEPSG)

    # Create a transform object to project WGS84 coordinates to user-specified output.
    transform = osr.CoordinateTransformation(in_srs, out_srs)

    # Create the track layer.
    track_layer = track_data_source.CreateLayer(out_line_filename, out_srs, ogr.wkbLineString)
    if track_layer is None:
        # Adjust to print function / python3 CH 20180107 (Add parens)
        print("\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting.")
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
    line_field_pre_sog = ogr.FieldDefn("sogpt1", ogr.OFTReal)
    line_field_pre_sog.SetWidth(5)
    line_field_pre_sog.SetPrecision(1)
    track_layer.CreateField(line_field_pre_sog)
    line_field_post_sog = ogr.FieldDefn("sogpt2", ogr.OFTReal)
    line_field_post_sog.SetWidth(5)
    line_field_post_sog.SetPrecision(1)
    track_layer.CreateField(line_field_post_sog)
    line_field_avg_sog = ogr.FieldDefn("avg_sog", ogr.OFTReal)
    line_field_avg_sog.SetWidth(5)
    line_field_avg_sog.SetPrecision(1)
    track_layer.CreateField(line_field_avg_sog)
    line_field_dist_speed = ogr.FieldDefn("distspdkts", ogr.OFTReal)
    line_field_dist_speed.SetWidth(5)
    line_field_dist_speed.SetPrecision(1)
    track_layer.CreateField(line_field_dist_speed)
    line_field_len = ogr.FieldDefn("seg_len_km", ogr.OFTReal)
    line_field_len.SetWidth(12)
    line_field_len.SetPrecision(6)
    track_layer.CreateField(line_field_len)

    # Initialize counters of the number of discarded and preserved segments.
    preserved_count = 0
    speed_discarded_count_fast = 0
    speed_discarded_count_slow = 0
    time_discarded_count = 0
    
    # Print a message about the stage of processing
    # Adjust to print function / python3 CH 20180107 (Add parens)
    print("Generating short (point-to-point) GIS layer from segmentation.\n")
    
    # Reset a counter into the input file.
    in_line_counter = 0

    # Create slices of odd [1,3,5,...,n-1] and even [2,4,6,...n] elements for pairwise comparison.
    first_pts_AIS_array = segment_split_array[0::2]
    second_pts_AIS_array = segment_split_array[1::2]

    # Create a projected (transformed) line object representation from a WKT build on each pair of elements
    track_wkts = map(create_segment_WKT, first_pts_AIS_array['longitude'], first_pts_AIS_array['latitude'], second_pts_AIS_array['longitude'], second_pts_AIS_array['latitude'])
    track_objs = map(lambda x: ogr.CreateGeometryFromWkt(x), track_wkts)
    track_transforms = map(lambda x: x.Transform(transform), track_objs)

### Log if transforms not successful.

    # Calculate elapsed seconds for each segment.
    elapsed_seconds = map(calc_time, first_pts_AIS_array['time'], second_pts_AIS_array['time'])

    # Calculate distance along each segment, using projected coordinate system, if available.
    if (outputEPSG != 4326):
        track_len_m = map(lambda x: x.Length(), track_objs)

    # If the segment is made of unprojected points, calculate its length using the haversine
    # formula (approximation of length), and corresponding speed in knots.
    else:
        track_len_m = map(haversine_geom_m, track_objs)

    # Calculate implied speeds 
    # CCC add (/ 0) protection here? Should be addressed in earlier processing, though 
    speed_knots = map(lambda x,y: (float(x) / float(1852)) / (float(y) / float(3600)), track_len_m, elapsed_seconds)

    # Identify segments with speeds between set thresholds
    valid_speeds = np.array(np.logical_and(np.greater(speed_knots,min_speed_bound_kts), np.less(speed_knots,max_speed_bound_kts)), dtype=bool)

    # Index subset of records with valid speeds. Perform different comprehension for lists.
    target_first_pts = first_pts_AIS_array[valid_speeds]
    target_second_pts = second_pts_AIS_array[valid_speeds]
    target_track_objs = compress(track_objs, valid_speeds)
    target_elapsed_seconds = compress(elapsed_seconds, valid_speeds)
    target_track_len_m = compress(track_len_m, valid_speeds)
    target_speed_knots = compress(speed_knots, valid_speeds)

### Print out data dropped due to invalid speeds here.

    # Print out as valid segments with speeds within the thresholds.
    map(lambda x,y,z,a,b,c: write_short_track_record(track_layer,x,y,z,a,b,c), target_first_pts, target_second_pts, target_track_objs, target_elapsed_seconds, target_track_len_m, target_speed_knots)

    # Destroy the data source to flush features to disk.
    track_data_source.Destroy()

    # Calculate the number of segments dropped due to implausible speeds, as well
    # as the number of segments created, and output.
    print("GIS representation constructed: \n\t {} further segments dropped for speed out of range \n\t {} total segments created.\n".format((len(first_pts_AIS_array) - len(target_first_pts)), len(target_first_pts))) 

# End Function generate_short_GIS

# Function segment_thresholded(dataframe_points, track_speed_threshold, in_track_separation_time)
def segment_thresholded(in_AIS_dataframe, track_speed_threshold, track_separation_time):

    # Create lists to hold the output data for each of the segment and discard dataframes.
    out_segment_AIS_items = []
    out_segment_AIS_items_segidx = []
    out_discarded_AIS_items = []
    out_discarded_AIS_items_reason = []

    # If the dataframe only contains a single point, output it as
    # stranded.
    if(in_AIS_dataframe.shape[0] == 1):
        
        out_discarded_AIS_items.append(np.asarray(in_AIS_dataframe.to_records(),[('index', '<i8'), ('time', '<M8[ns]'), ('message_id', '<i8'), ('mmsi', '<i8'), ('navigational_status', '<i8'), ('sog', '<f8'), ('cog', '<f8'), ('heading', '<f8'), ('latitude', '<f8'), ('longitude', '<f8')])[0])
        out_discarded_AIS_items = append_fields(out_discarded_AIS_items, 'reason', np.asarray(["Stranded single point"]))

        return(out_segment_AIS_items, out_discarded_AIS_items)

    # Establish a segment counter.
    segment_counter = 0

    # Translate the incoming dataframe to a Numpy NDarray of predictable dtype
    in_AIS_data_array = np.asarray(in_AIS_dataframe.to_records(),[('index', '<i8'), ('time', '<M8[ns]'), ('message_id', '<i8'), ('mmsi', '<i8'), ('navigational_status', '<i8'), ('sog', '<f8'), ('cog', '<f8'), ('heading', '<f8'), ('latitude', '<f8'), ('longitude', '<f8')])

    ### DEBUG
    """print("Initial points:")
    print(in_AIS_data_array)
    print(in_AIS_data_array.shape)"""
    ### DEBUG

    # Identify points with bad coordinates to be dropped.
    invalid_lat = np.array(abs(in_AIS_data_array['latitude']) > 90, dtype=bool)
    invalid_lon = np.array(abs(in_AIS_data_array['longitude']) > 180, dtype=bool)
    invalid_coord = np.where(np.logical_or(invalid_lat, invalid_lon))
    
    # Copy invalid coordinates to output.
    invalid_records = in_AIS_data_array[invalid_coord]

    # Copy records w/ invalid coordinates to discarded items.
    if (invalid_records.shape[0] > 0):
        out_discarded_AIS_items.extend(invalid_records)
        out_discarded_AIS_items_reason.extend(["Out of range"] * invalid_records.shape[0])

    # Delete invalid coordinates from array.
    in_AIS_data_array = np.delete(in_AIS_data_array,invalid_coord,0)

    ### DEBUG
    """print("Points after invalid deletion:")
    print(in_AIS_data_array)
    print(in_AIS_data_array.shape)"""
    ### DEBUG

    # If there are less than three points, handle them separately than the remainder.
    if (in_AIS_data_array.shape[0] < 3):

### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output
### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output
### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output

        ### DEBUG
#        print("Less than three points remain for trajectory after discarding invalid.")
        ### DEBUG

        # Append the points to the discard array.
        out_discarded_AIS_items.extend(in_AIS_data_array)
        out_discarded_AIS_items_reason.extend(["Not enough points for segmentation"] * in_AIS_data_array.shape[0])

        # Merge the data and labels from the lists of discarded and preserved records into
        # the output arrays. If no records exist retain empty arrays.
        if(len(out_segment_AIS_items) > 0):

            out_segment_AIS_items = append_fields(np.asarray(out_segment_AIS_items), 'segidx', np.asarray(out_segment_AIS_items_segidx))
            
        if(len(out_discarded_AIS_items) > 0):
            
            out_discarded_AIS_items = append_fields(np.asarray(out_discarded_AIS_items), 'reason', np.asarray(out_discarded_AIS_items_reason))

        return (out_segment_AIS_items, out_discarded_AIS_items)

    # Create slices of [1,n-1] and [2,n] elements for pairwise comparison.
    first_pts_AIS_array = in_AIS_data_array[0:-1]
    second_pts_AIS_array = in_AIS_data_array[1:]

    # Calculate elapsed times between the two arrays.
    elapsed_times = map(calc_time, first_pts_AIS_array['time'], second_pts_AIS_array['time'])

    # Calculate duplicate positions in points between the two arrays.
    dup_positions = np.logical_and(np.equal(first_pts_AIS_array['longitude'], second_pts_AIS_array['longitude']), np.equal(first_pts_AIS_array['latitude'], second_pts_AIS_array['latitude']))

    # Identify duplicates on coordinate, time through the two computed arrays
    dup_indices = np.array(np.logical_or((np.equal(elapsed_times, 0)), (dup_positions)), dtype=bool)

    ### DEBUG
    """print("Duplicate pos:")
    print(dup_positions)
    print(dup_positions.shape)
    print("Duplicate indices:")
    print(dup_indices)
    print(dup_indices.shape)"""
    ### DEBUG

    # Select duplicates and add to discarded output (both)
    duplicate_first_points = first_pts_AIS_array[dup_indices]
    duplicate_second_points = second_pts_AIS_array[dup_indices]

    # Copy records from duplicate outputs to discarded items.
    if (duplicate_first_points.shape[0] > 0):
        out_discarded_AIS_items.extend(duplicate_first_points)
        out_discarded_AIS_items_reason.extend(["Duplicate point/time (pt1's)"] * duplicate_first_points.shape[0])
        
    if (duplicate_second_points.shape[0] > 0):
        out_discarded_AIS_items.extend(duplicate_second_points)
        out_discarded_AIS_items_reason.extend(["Duplicate point/time (pt2's)"] * duplicate_second_points.shape[0])

    # Delete out duplicates.
    first_pts_AIS_array_nodups = first_pts_AIS_array[np.logical_not(dup_indices)]
    second_pts_AIS_array_nodups = second_pts_AIS_array[np.logical_not(dup_indices)]

    ### DEBUG
    """print("Non Duplicate pos (1):")
    print(first_pts_AIS_array_nodups)
    print(first_pts_AIS_array_nodups.shape)
    print("Non Duplicate pos (2):")
    print(second_pts_AIS_array_nodups)
    print(second_pts_AIS_array_nodups.shape)"""
    ### DEBUG

### If deleting duplicates results in less than 3 points, handle this case.
    if (first_pts_AIS_array_nodups.shape[0] < 2):

### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output
### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output
### Handle this case gracefully - maybe re-use / test point pair?.
### ADD Points to output

        ### DEBUG
#        print("Less than three points remain for trajectory after discarding duplicates.")
        ### DEBUG

        # Append any points to the discard array.
        if(first_pts_AIS_array_nodups.shape[0] > 0):
            out_discarded_AIS_items.extend(first_pts_AIS_array_nodups)
            out_discarded_AIS_items_reason.extend(["Not enough points for segmentation"] * first_pts_AIS_array_nodups.shape[0])
            out_discarded_AIS_items.extend(second_pts_AIS_array_nodups[-1])
            out_discarded_AIS_items_reason.extend(["Not enough points for segmentation"] * 1)

        # Merge the data and labels from the lists of discarded and preserved records into
        # the output arrays. If no records exist retain empty arrays.
        if(len(out_segment_AIS_items) > 0):

            out_segment_AIS_items = append_fields(np.asarray(out_segment_AIS_items), 'segidx', np.asarray(out_segment_AIS_items_segidx))
            
        if(len(out_discarded_AIS_items) > 0):
            
            out_discarded_AIS_items = append_fields(np.asarray(out_discarded_AIS_items), 'reason', np.asarray(out_discarded_AIS_items_reason))

        # Add any points remaining to output (should be 2 or 1).
        return (out_segment_AIS_items, out_discarded_AIS_items)

    # Restructure to determine points with excessive speed (ORDER CRITICAL FOR PTS 2/3)

    #Rebuild array of remaining points, then split into triples
    remaining_points = np.hstack((first_pts_AIS_array_nodups, second_pts_AIS_array_nodups[-1]))

    first_pts_AIS_array_nodups = remaining_points[:-2]
    second_pts_AIS_array_nodups = remaining_points[1:-1]
    third_pts_AIS_array_nodups = remaining_points[2:]
    
    # Calculate elapsed times and between 1st and 2nd and 2nd and 3rd arrays.
    elapsed_times_1_2 = map(calc_time, first_pts_AIS_array_nodups['time'], second_pts_AIS_array_nodups['time'])
    elapsed_times_2_3 = map(calc_time, second_pts_AIS_array_nodups['time'], third_pts_AIS_array_nodups['time'])

    # Calculate elapsed distances between 1st and 2nd and 2nd and 3rd arrays.
    elapsed_distances_1_2 = map(haversine_coords_km, first_pts_AIS_array_nodups['longitude'], first_pts_AIS_array_nodups['latitude'], second_pts_AIS_array_nodups['longitude'], second_pts_AIS_array_nodups['latitude'])
    elapsed_distances_2_3 = map(haversine_coords_km, second_pts_AIS_array_nodups['longitude'], second_pts_AIS_array_nodups['latitude'], third_pts_AIS_array_nodups['longitude'], third_pts_AIS_array_nodups['latitude'])

    # Calculate elapsed speeds in km/second between 1st and 2nd and 2nd and 3rd arrays.
    elapsed_speeds_1_2 = map(lambda x,y: x/y, elapsed_distances_1_2, elapsed_times_1_2)
    elapsed_speeds_2_3 = map(lambda x,y: x/y, elapsed_distances_2_3, elapsed_times_2_3)

    # Identify point#2 values where the speeds from 1->2 and 2->3 both exceed the established threshold.
    excess_speed_indices = np.array(np.logical_and((np.greater(elapsed_speeds_1_2, track_speed_threshold)), (np.greater(elapsed_speeds_2_3, track_speed_threshold))), dtype=bool)    

    # Test the first and last points as one sided tests.
    """
    excess_speed_indices[0] = (elapsed_speeds_1_2[0] > track_speed_threshold)
    excess_speed_indices[-1] = (elapsed_speeds_2_3[-1] > track_speed_threshold)
    """

    # Select positions with excess speed and add to discarded output.
    invalid_speed_points = second_pts_AIS_array_nodups[excess_speed_indices]

    ### DEBUG
    """print("Invalid speed:")
    print(invalid_speed_points)
    print(invalid_speed_points.shape)"""
    ### DEBUG

    # Copy records w/ suggesting invalid speed to discarded items.
    if (invalid_speed_points.shape[0] > 0):
        out_discarded_AIS_items.extend(invalid_speed_points)
        out_discarded_AIS_items_reason.extend(["Excess speed"] * invalid_speed_points.shape[0])

    # Delete out excessive speed records.
    first_pts_AIS_array_nodups_nobadspeed = first_pts_AIS_array_nodups[np.logical_not(excess_speed_indices)]
    second_pts_AIS_array_nodups_nobadspeed = second_pts_AIS_array_nodups[np.logical_not(excess_speed_indices)]
    third_pts_AIS_array_nodups_nobadspeed = third_pts_AIS_array_nodups[np.logical_not(excess_speed_indices)]

    ### DEBUG
    """print("Valid pts array sizes:")
    print(first_pts_AIS_array_nodups_nobadspeed.shape)
    print(second_pts_AIS_array_nodups_nobadspeed.shape)
    print(third_pts_AIS_array_nodups_nobadspeed.shape)"""
    ### DEBUG
  
    # Build new point list, based on first point of 1 + all of 2 + last point of 3;
    new_pts_AIS_array = np.hstack((first_pts_AIS_array_nodups_nobadspeed[0],second_pts_AIS_array_nodups_nobadspeed, third_pts_AIS_array_nodups_nobadspeed[-1]))

    ### DEBUG
    """print("Remaining points:")
    print(new_pts_AIS_array)
    print(new_pts_AIS_array.shape)"""
    ### DEBUG

### If filtering on speed results in less than 3 points, handle this case.
    if (new_pts_AIS_array.shape[0] < 3):

### Handle this case gracefully - either re-use / test point pair, or output as discarded?.
### Handle this case gracefully - either re-use / test point pair, or output as discarded?.
### Handle this case gracefully - either re-use / test point pair, or output as discarded?.
### ADD Points to output
### ADD Points to output
### ADD Points to output

        ### DEBUG
#        print("Less than three points remain for trajectory after discarding excessive speed points.")
        ### DEBUG

        # Append any points to the discard array.
        if(new_pts_AIS_array.shape[0] > 0):
            out_discarded_AIS_items.extend(new_pts_AIS_array)
            out_discarded_AIS_items_reason.extend(["Not enough points for segmentation"] * new_pts_AIS_array.shape[0])

        # Merge the data and labels from the lists of discarded and preserved records into
        # the output arrays. If no records exist retain empty arrays.
        if(len(out_segment_AIS_items) > 0):

            out_segment_AIS_items = append_fields(np.asarray(out_segment_AIS_items), 'segidx', np.asarray(out_segment_AIS_items_segidx))
            
        if(len(out_discarded_AIS_items) > 0):
            
            out_discarded_AIS_items = append_fields(np.asarray(out_discarded_AIS_items), 'reason', np.asarray(out_discarded_AIS_items_reason))

        # Add any points remaining to output (should be 2 or 1).
        return (out_segment_AIS_items, out_discarded_AIS_items)

    # Rebuild pairs from point list, to determine stopping.
    first_pts_AIS_array_stoptest = new_pts_AIS_array[:-1]
    second_pts_AIS_array_stoptest = new_pts_AIS_array[1:]

    # Calculate elapsed times and between 1st and 2nd and 2nd and 3rd arrays.
    elapsed_stop_times_1_2 = map(calc_time, first_pts_AIS_array_stoptest['time'], second_pts_AIS_array_stoptest['time'])

    # Calculate elapsed distances between 1st and 2nd and 2nd and 3rd arrays.
    elapsed_stop_distances_1_2 = map(haversine_coords_km, first_pts_AIS_array_stoptest['longitude'], first_pts_AIS_array_stoptest['latitude'], second_pts_AIS_array_stoptest['longitude'], second_pts_AIS_array_stoptest['latitude'])

    # Calculate elapsed speeds in knots between 1st and 2nd and 2nd and 3rd arrays.
    # Speed in km/s here   elapsed_stop_speeds_1_2 = map(lambda x,y: x/y, elapsed_stop_distances_1_2, elapsed_stop_times_1_2)
# Oops should be division for km -> nmi   elapsed_stop_speeds_1_2 = map(lambda x,y: (float(x) * float(1.852)) / (float(y) / float(3600)), elapsed_stop_distances_1_2, elapsed_stop_times_1_2)
    elapsed_stop_speeds_1_2 = map(lambda x,y: (float(x) / float(1.852)) / (float(y) / float(3600)), elapsed_stop_distances_1_2, elapsed_stop_times_1_2)

    # Label points as stopping:
    # Identify from point pairs locations where either: a) inter-point 
    # speed values indicate stopping due to low speed, b) or stopping due to 
    # elapsed time.
    stopping_speed_indices = np.array(np.logical_or((np.less(elapsed_stop_speeds_1_2, min_speed_bound_kts)), (np.greater(elapsed_stop_times_1_2, track_separation_time))), dtype=bool)    


    ### DEBUG
    """print("Stop calculations:")
    print(elapsed_stop_times_1_2)
    print(elapsed_stop_speeds_1_2)
    print(stopping_speed_indices)
    print(new_pts_AIS_array.shape)"""
    ### DEBUG

    # Initialize prior point as a stop (i.e. start in "stopped" state) and set 
    # starting segment index value to 0.
    prior_stop = True
    seg_idx = 0

    # Initialize an array for working aggregation.
    work_agg_out_segment_AIS_items = []
    work_agg_out_segment_AIS_items_segidx = []

### DEBUG 
#    print("Iteration range: 0 : {}".format(stopping_speed_indices.shape[0]))
    """print ("Stop times")
    print (elapsed_stop_times_1_2[0:10])
    print (track_separation_time)

    print ("Stop speeds")
    print (elapsed_stop_speeds_1_2[0:10])
    print (track_separation_time)

    print ("Stop indices")
    print (stopping_speed_indices[0:10])
    print (first_pts_AIS_array_stoptest[0:3])
    print (second_pts_AIS_array_stoptest[0:3])"""
### DEBUG 



    # Iterate over the stopping indicators / first n-1 elements to be segmented.
    for i in range(stopping_speed_indices.shape[0]):
        
        # If the prior element tested was a stop:
        if prior_stop:
            
            # If the current point pair is a stop, discard the current point as a stranded stop.
            if (stopping_speed_indices[i]):
                
# Fixed
                out_discarded_AIS_items.append(first_pts_AIS_array_stoptest[i])
                out_discarded_AIS_items_reason.extend(["Stranded stop"])
            
            # Otherwise, set the flag to indicate prior is not a stop, append the point to an aggregation.
            else:

                prior_stop = False

                # Append the current point and segment index to the aggregation
# Fixed
                work_agg_out_segment_AIS_items.append(first_pts_AIS_array_stoptest[i])
                work_agg_out_segment_AIS_items_segidx.extend([seg_idx])
                
                
        # Otherwise, the prior element tested was not as stop:
        else:
    
            # If the current point pair is not a stop, append the point to an aggregation.
            if not (stopping_speed_indices[i]):

                # Append the current point and segment index to the aggregation.
# Fixed
                work_agg_out_segment_AIS_items.append(first_pts_AIS_array_stoptest[i])
                work_agg_out_segment_AIS_items_segidx.extend([seg_idx])

            # Otherwise write out the current point set and set prior as a stop.
            else:

                # Append the current point and segment index to the aggregation
# Fixed
                work_agg_out_segment_AIS_items.append(first_pts_AIS_array_stoptest[i])
                work_agg_out_segment_AIS_items_segidx.extend([seg_idx])
                
                # Copy the aggregation to the output.
# Fixed -- test
                out_segment_AIS_items.extend(work_agg_out_segment_AIS_items)
                out_segment_AIS_items_segidx.extend(work_agg_out_segment_AIS_items_segidx)
                
                # Create a new, empty aggregation.
                work_agg_out_segment_AIS_items = []
                work_agg_out_segment_AIS_items_segidx = []

                # Increment the segment index for the next segment.
                seg_idx = seg_idx + 1
                
                # Set the flag to indicate prior was a stop.
                prior_stop = True

    # If there is a stop prior to the end of the set of points, output the last point
    # as a stranded stop.
    if (prior_stop):
    
        out_discarded_AIS_items.append(second_pts_AIS_array_stoptest[-1])
        out_discarded_AIS_items_reason.extend(["Stranded stop"])
# Edit #1        out_discarded_AIS_items.append(second_pts_AIS_array_stoptest[-1])
# Edit #1        out_discarded_AIS_items_reason.append(["Stranded stop"])


    # Otherwise, there is a pending aggregation at the end of iteration, and
    # the final point should be appended.
    else:

        """ Presumed typo - failure to add last "hanging" segment.
        work_agg_out_segment_AIS_items.extend(second_pts_AIS_array_stoptest[-1])
        work_agg_out_segment_AIS_items_segidx.extend([seg_idx])
        """
        # Append the last point and segment index to the aggregation
        work_agg_out_segment_AIS_items.append(second_pts_AIS_array_stoptest[-1])
        work_agg_out_segment_AIS_items_segidx.extend([seg_idx])
                
        # Copy the aggregation to the output.
        out_segment_AIS_items.extend(work_agg_out_segment_AIS_items)
        out_segment_AIS_items_segidx.extend(work_agg_out_segment_AIS_items_segidx)


    ### DEBUG
    """print("Segmentation result segments:")
    print(out_segment_AIS_items)
    print(out_segment_AIS_items)
    print("Segmentation result discarded:")
    print(out_discarded_AIS_items)"""
    ### DEBUG


    # Merge the data and labels from the lists of discarded and preserved records into
    # the output arrays. If no records exist retain empty arrays.
    if(len(out_segment_AIS_items) > 0):

        out_segment_AIS_items = append_fields(np.asarray(out_segment_AIS_items), 'segidx', np.asarray(out_segment_AIS_items_segidx))
        
    if(len(out_discarded_AIS_items) > 0):
        
        out_discarded_AIS_items = append_fields(np.asarray(out_discarded_AIS_items), 'reason', np.asarray(out_discarded_AIS_items_reason))

    
    ### DEBUG
    """print("Segmentation result segments:")
    print("Number of segment points: {}, number of discards: {}".format(len(out_segment_AIS_items), len(out_discarded_AIS_items)))"""
    ### DEBUG

    # Return the output arrays.
    return (out_segment_AIS_items, out_discarded_AIS_items)

# End Function segment_thresholded

# Function generate_threshold_tracks - Generate trajectories from point 
# sequences using thresholds max speed and point separation.
def generate_threshold_tracks(source_dataframe, segment_split_directory, segment_other_split_directory, track_separation_time, max_point_speed):

    # Convert the max speed value in kph to kilometres per second to match with script.
    track_speed_threshold = max_point_speed / float(3600)
    
    # Select the mmsi records in sorted order on mmsi.
    dataframe_mmsis = source_dataframe['mmsi'].unique()

    #Establish a progressbar for iterating over incoming records
    in_records_bar = progressbar.ProgressBar()

    # Establish an overall array to hold all segmented records.
    all_generated_segments_array = None

    # Establish values to track the number of points discarded as each of
    # duplicate points, time threshold exceeded and stranded points
    dup_discards = 0
    time_discards = 0
    stranded_discards = 0

    # Establish a value to track the total number of input points.
    total_points = 0
    
    # Iterate over the incoming dataframe records, processing each vessel / mmsi in
    # sequence while tracking progress with a progressbar.
    with progressbar.ProgressBar(max_value=dataframe_mmsis.size) as in_data_bar:
        mmsi_iterator = np.nditer(dataframe_mmsis, flags=['f_index'])
        while not mmsi_iterator.finished:

            # Copy the current mmsi value from the iterator.
            mmsidatarow = mmsi_iterator[0]

            # Use the current mmsi to fetch a second dataframe holding the point data 
            # for the current vessel, sorted on date / message_id / lat / lon.
            dataframe_points = source_dataframe.query('mmsi == @mmsidatarow')
            dataframe_points.sort_values(by=['time', 'message_id', 'latitude', 'longitude', 'cog', 'sog', 'heading'])
            
            # Update the number of total input points
            total_points += dataframe_points.shape[0]

            # Request segmentation of the point data.
            (segments_array, discards_array) = segment_thresholded(dataframe_points, track_speed_threshold, track_separation_time)

            if (len(segments_array) > 0):
                
                try:
                    out_vessel_records = open(segment_split_directory + sep + str(mmsidatarow) + ".txt", 'w')
                
                except IOError:
                    print("Error opening file: " + segment_split_directory + sep + str(mmsidatarow) + ".txt" + "\n")
                    quit()

                for pointdatarow in np.nditer(segments_array): 
                    
                    lineout = "{},{},{},{},{},{},{},{:.0f},{},{}\n".format(pointdatarow['segidx'], pointdatarow['time'], pointdatarow['message_id'], pointdatarow['mmsi'], pointdatarow['navigational_status'], pointdatarow['sog'], pointdatarow['cog'], pointdatarow['heading'], pointdatarow['latitude'], pointdatarow['longitude'])
                    out_vessel_records.write(lineout)

                # Close the output file.
                out_vessel_records.close()
            
            # Output the discarded points to the invalid datafile.
            if (len(discards_array) > 0):
                
                try:
                    out_invalid_records = open(segment_other_split_directory + sep + "invalid_" + str(mmsidatarow) + ".txt", 'w')
                
                except IOError:
                    print("Error opening file: " + segment_other_split_directory + sep + "invalid_" + str(mmsidatarow) + ".txt" + "\n")
                    quit()

                for pointdatarow in np.nditer(discards_array): 
                    
                    lineout = "{},{},{},{},{},{},{},{},{},{}\n".format(pointdatarow['time'], pointdatarow['message_id'], pointdatarow['mmsi'], pointdatarow['navigational_status'], pointdatarow['sog'], pointdatarow['cog'], pointdatarow['heading'], pointdatarow['latitude'], pointdatarow['longitude'], pointdatarow['reason'])
                    out_invalid_records.write(lineout)

                # Close the output file.
                out_invalid_records.close()
                
                # Calculate the number of various discard types from the records returned.
                dup_discards = dup_discards + len(np.where(discards_array['reason'] == "Duplicate point/time (pt1's)")[0])
                stranded_discards = stranded_discards + len(np.where(discards_array['reason'] == "Stranded stop")[0])
                few_points_discards = stranded_discards + len(np.where(discards_array['reason'] == "Not enough points for segmentation")[0])
                overspeed_discards = stranded_discards + len(np.where(discards_array['reason'] == "Excess speed")[0])

            # If there are any current segment records, incorporate them into the overall list.
            if(all_generated_segments_array is None or (len(all_generated_segments_array) == 0)):

                all_generated_segments_array = segments_array

            elif (len(segments_array) > 0):
                
                all_generated_segments_array = np.concatenate((all_generated_segments_array,segments_array))

            # Update the Progressbar using the index from the iterator.
            in_data_bar.update(mmsi_iterator.index)

            # Increment the iterator.
            mmsi_iterator.iternext()

    # Print a message indicating the type and number of discards in the segmentation.
    print("Thresholded segmentation complete:\n\t {} input points, \n\t {} stranded points discarded, \n\t {} points discarded from duplicate start/end, \n\t {} segments discarded as due to too few points in set and \n\t {} points discarded due to overspeed. Time threshold used: {} seconds.\n".format(total_points, stranded_discards,dup_discards,few_points_discards,overspeed_discards,track_separation_time))

    # Return the segmented track data.
    return all_generated_segments_array

# End Function generate_threshold_tracks

# Function generate_threshold_GIS - Write out a GIS representation of the output
# of a threshold based segmentation.
def generate_threshold_GIS(segment_split_array, gis_directory, out_filename_prefix, outputEPSG):

    # Copy the output filename from the argument vector.
    out_line_filename = os.path.basename(gis_directory + sep + out_filename_prefix + "_lines")
    out_line_directory = os.path.dirname(gis_directory + sep + out_filename_prefix + "_lines")

    # Set up the shapefile driver.
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Create the data source.
    track_data_source = driver.CreateDataSource(out_line_directory)

    # create the spatial reference, WGS84
    in_srs = osr.SpatialReference()
    in_srs.ImportFromEPSG(inputEPSG)

    # Create the output spatial reference, BC Albers
    # Presuming effective equality between NAD83 / WGS84 over function scope (e.g. North America -- https://www.packtpub.com/books/content/working-geo-spatial-data-python)
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(outputEPSG)

    # Create a transform object to project WGS84 coordinates to user-specified output.
    transform = osr.CoordinateTransformation(in_srs, out_srs)
    
    # Create the track and point layers.
    track_layer = track_data_source.CreateLayer(out_line_filename, out_srs, ogr.wkbLineString)
    if track_layer is None:
        print("\nError encountered when creating output track shapefile: " + out_line_directory + "\\" + out_line_filename + " \nAborting.")
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
    track_layer.CreateField(ogr.FieldDefn("seg_pts", ogr.OFTInteger))
    track_field_avg_sog = ogr.FieldDefn("avg_sog", ogr.OFTReal)
    track_field_avg_sog.SetWidth(12)
    track_field_avg_sog.SetPrecision(6)
    track_layer.CreateField(track_field_avg_sog)
    track_field_med_sog = ogr.FieldDefn("med_sog", ogr.OFTReal)
    track_field_med_sog.SetWidth(12)
    track_field_med_sog.SetPrecision(6)
    track_layer.CreateField(track_field_med_sog)       
    track_field_imp_spd = ogr.FieldDefn("dstspdkts", ogr.OFTReal)
    track_field_imp_spd.SetWidth(12)
    track_field_imp_spd.SetPrecision(6)
    track_layer.CreateField(track_field_imp_spd)
    track_field_seg_len_km = ogr.FieldDefn("seg_len_km", ogr.OFTReal)
    track_field_seg_len_km.SetWidth(12)
    track_field_seg_len_km.SetPrecision(6)
    track_layer.CreateField(track_field_seg_len_km)

    # Print a message about the stage of processing
    # Adjust to print function / python3 CH 20180107 (Add parens)
    print("\nGenerating thresholded track GIS layer generation.")
    
    #Establish a progressbar for iterating over incoming data
    # Iterate over the incoming split array records, processing each vessel / mmsi in
    # sequence while tracking progress with a progressbar.
    
    ### DEBUG 
#    print len(segment_split_array)
#    print(segment_split_array.shape[0])
#    print(segment_split_array)
    ### DEBUG     
    with progressbar.ProgressBar(max_value=segment_split_array.shape[0]) as in_segment_vessels_bar:

        # Fetch the first set of values from the incoming split array.
        curr_mmsi = segment_split_array['mmsi'][0]
        curr_segidx = segment_split_array['segidx'][0]
        start_idx = 0

        # Iterate over the divided files generated in the previous step.
        for i in range(segment_split_array.shape[0]):

            # If the current mmsi and segment match the existing ones, iterate 
            # to the next element.
            if(curr_mmsi != segment_split_array['mmsi'][i] or curr_segidx != segment_split_array['segidx'][i]):

### DEBUG
#                print("Processing: MMSI {} - Segment {} - Rownumber {}".format(curr_mmsi, curr_segidx, i))
### DEBUG

                
                # Create a segment between the start_idx and i-1 elements.
# Error in range indexing               segment_elements = segment_split_array[start_idx:i-1]
                segment_elements = segment_split_array[start_idx:i]
                
                # Compute mean and median sog values.
                curr_mean = np.nanmean(segment_elements['sog'])
                curr_median = np.nanmedian(segment_elements['sog'])
                
                # Establish an wkt of the points 
                track_wkt = create_threshold_traj_WKT(segment_elements['longitude'], segment_elements['latitude'])


                # Reproject the WKT
### Log if transforms not successful.
                track_obj = ogr.CreateGeometryFromWkt(track_wkt)
                track_obj.Transform(transform)

                # Calculate distance along each segment, using projected coordinate system, if available.
                if (outputEPSG != 4326):
                    track_len_m = track_obj.Length()

                # If the trajectory is made of unprojected points, calculate its length using the haversine
                # formula (approximation of length), and corresponding speed in knots.
                else:
                    # Compute the length as the sum of segment lengths over the trajectory.
                    track_len_m = np.sum(map(haversine_coords_km, segment_elements['longitude'][:-1], segment_elements['latitude'][:-1], segment_elements['longitude'][1:], segment_elements['latitude'][1:])) * float(1000)

                # Calculate elapsed seconds for each segment.
                elapsed_seconds = calc_time(segment_elements['time'][0], segment_elements['time'][-1])

### DEBUG
#                print("Track len m: {}, elapsed_seconds {}, start {}, end {}, size {}".format(track_len_m, elapsed_seconds, start_idx, i-1, segment_elements.shape[0]))
### DEBUG

                # Calculate implied speed
                implied_speed_knots = (float(track_len_m) / float(1852)) / (float(elapsed_seconds) / float(3600))

                # Generate a feature and and populate its fields.
                track_feature = ogr.Feature(track_layer.GetLayerDefn())
                track_feature.SetField("TrackID" ,curr_segidx)
                track_feature.SetField("mmsi" ,curr_mmsi)
                track_feature.SetField("elp_sec" , elapsed_seconds)
                track_feature.SetField("seg_pts", segment_elements.shape[0])
                # The date conversion below is madness, but apparrently the way to go: https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64 
                track_feature.SetField("st_date" , pd.to_datetime(str(segment_elements['time'][0])).strftime("%Y-%m-%d %H:%M:%S"))
                track_feature.SetField("en_date" , pd.to_datetime(str(segment_elements['time'][-1])).strftime("%Y-%m-%d %H:%M:%S"))
                track_feature.SetField("avg_sog" , curr_mean)
                track_feature.SetField("med_sog" , curr_median)
                track_feature.SetField("dstspdkts" , implied_speed_knots)
                track_feature.SetField("seg_len_km" , track_len_m / float(1000))

                # Assign the track geometry and to the feature.                
                track_feature.SetGeometry(track_obj)
                
                # Create the feature within the output layer, then reclaim assigned memory.
                track_layer.CreateFeature(track_feature)
                track_feature.Destroy()

                # Update the current mmsi / segment index; start index values
                curr_mmsi = segment_split_array['mmsi'][i]
                curr_segidx = segment_split_array['segidx'][i]
                start_idx = i

            # Update the progressbar
            in_segment_vessels_bar.update(i)

                
        # Finalize the last segment between the current start_idx and the last element.
        segment_elements = segment_split_array[start_idx:]
        
        # Compute mean and median sog values.
        curr_mean = np.mean(segment_elements['sog'])
        curr_median = np.median(segment_elements['sog'])
        
        # Establish an wkt of the points 
        track_wkt = create_threshold_traj_WKT(segment_elements['longitude'], segment_elements['latitude'])

        # Reproject the WKT
### Log if transforms not successful.
        track_obj = ogr.CreateGeometryFromWkt(track_wkt)
        track_obj.Transform(transform)

        # Calculate distance along each segment, using projected coordinate system, if available.
        if (outputEPSG != 4326):
            track_len_m = track_obj.Length()

        # If the trajectory is made of unprojected points, calculate its length using the haversine
        # formula (approximation of length), and corresponding speed in knots.
        else:
            track_len_m = np.sum(map(haversine_coords_km, segment_elements['longitude'][:-1], segment_elements['latitude'][:-1], segment_elements['longitude'][1:], segment_elements['latitude'][1:])) * float(1000)

        # Calculate elapsed seconds for each segment.
        elapsed_seconds = calc_time(segment_elements['time'][0], segment_elements['time'][-1])

        # Calculate implied speed
        implied_speed_knots = (float(track_len_m) / float(1852)) / (float(elapsed_seconds) / float(3600))

        # Generate a feature and and populate its fields.
        track_feature = ogr.Feature(track_layer.GetLayerDefn())
        track_feature.SetField("TrackID" ,curr_segidx)
        track_feature.SetField("mmsi" ,curr_mmsi)
        track_feature.SetField("elp_sec" , elapsed_seconds)
        track_feature.SetField("seg_pts", segment_elements.shape[0])
        # The date conversion below is madness, but apparrently the way to go: https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64 
        track_feature.SetField("st_date" , pd.to_datetime(str(segment_elements['time'][0])).strftime("%Y-%m-%d %H:%M:%S"))
        track_feature.SetField("en_date" , pd.to_datetime(str(segment_elements['time'][-1])).strftime("%Y-%m-%d %H:%M:%S"))
        track_feature.SetField("avg_sog" , curr_mean)
        track_feature.SetField("med_sog" , curr_median)
        track_feature.SetField("dstspdkts" , implied_speed_knots)
        track_feature.SetField("seg_len_km" , track_len_m / float(1000))


        # Assign the track geometry and to the feature.                
        track_feature.SetGeometry(track_obj)
        
        # Create the feature within the output layer, then reclaim assigned memory.
        track_layer.CreateFeature(track_feature)
        track_feature.Destroy()

    # Destroy the data sources to flush features to disk.
    track_data_source.Destroy()

# End Function generate_threshold_GIS

# Usage string for the script.
usage_string = """Usage: 1_generate_tracks_from_MEOPAR_AIS.py output_directory output_filename_prefix input_type input_name table:connect_string_filename outputEPSG short_track_indicator 0:[track_separation_time max_point_speed]|1:[max_elapsed_time]

Splits Parsed AIS record data (Postgres Table - sourced) by mmsi (vessel), then segments positions and generates GIS output layers. Presumes that the outputdirectory can be created, but does not yet exist. Requires GDAL/OGR/OSR, and progressbar2 packages for Python and a database schema configured as per the MEOPAR AIS data asset. Developed in support of the the exactEarth SAIS data initiative of MEOPAR (http://www.meopar.ca/).

input_type -              The type of input to be used, either "text" for file-
                          based, or "table" for DB-table based.
input_name -              The location/name of the input table in the MEOPAR/eE/
                          Dal S-AIS database. Presumed to contain fields: (time,
                          message_id, mmsi, navigational_status, sog, cog, 
                          heading, latitude, longitude).
connect_string_filename - The location of a file containing a single 
                          connectstring in .pgpass file format for the database 
                          in which the input table resides.
outputEPSG -              The EPSG identifier for the output projection / 
                          coordinate system of the GIS data. Defaults to WGS 85 
                          / EPSG 4326
short_track_indicator -   A flag, set as 1 to indicate point to point segment 
                          generation, and 0 for time and speed based track 
                          generation.
track_separation_time -   (only for short_track_indicator == 0) For track 
                          generation, the maximum time interval (in seconds) to 
                          be permitted between subsequent points in a track.
max_point_speed -         (only for short_track_indicator == 0) - For track 
                          generation, the maximum speed allowed (in kph), over 
                          which points with such speed are dropped as erroneous.
max_elapsed_time -        (only for short_track_indicator == 1) - For segment 
                          generation, the maximum length of segment (in seconds)
                          to be considered valid.

"""

def main():

    p = argparse.ArgumentParser(description='(1_generate_tracks_from_MEOPAR_AIS.py) Splits Parsed AIS record data (Postgres Table - sourced) by mmsi (vessel), then segments positions and generates GIS output layers. Presumes that the outputdirectory can be created, but does not yet exist. Presumes coordinate system units of metres (m), or WGS84 (EPSG 4326). Requires GDAL/OGR/OSR, and progressbar2 packages for Python and a database schema configured as per the MEOPAR AIS data asset. Developed in support of the the exactEarth SAIS data initiative of MEOPAR (http://www.meopar.ca/).')
    p.add_argument('outdirectory', type=str, help='The target directory into which output results will be generated.')
    p.add_argument('out_filename_prefix', type=str, help='The base filename prefix under which the output should be written.')
    group1 = p.add_mutually_exclusive_group(required=True)
    group1.add_argument('--textin', type=str, nargs=1, dest='IN_FILENAME', required=False, help='Indicates that the input will be in text form. IN_FILENAME is location of the text input file. Only one of --textin or --dbin may be selected.')
    group1.add_argument('--dbin', type=str, nargs=2, metavar=('IN_TABLENAME','CONNECT_STRING_FILENAME'), required=False, help='Indicates that the input will be a db table. IN_TABLENAME is the schema.tablename of the input data table, while CONNECT_STRING_FILENAME is a reference to a .pgpass connect string file. Only one of --textin or --dbin may be selected.')
    group2 = p.add_mutually_exclusive_group(required=True)
    group2.add_argument('--shorttrk', nargs=1, type=float, dest='MAX_ELAPSED_TIME', required=False, help='Indicates that point-to-point tracks should be created. MAX_ELAPSED_TIME is the maximum time interval (in seconds) to be permitted between subsequent points in a track. Only one of --shorttrk or --fulltrk may be selected.')
    group2.add_argument('--fulltrk', nargs=2, type=float, metavar=('TRACK_SEPARATION_TIME','MAX_POINT_SPEED'), required=False, help='Indicates that full vessels paths should be created. TRACK_SEPARATION_TIME is the maximum time interval (in seconds) to be permitted between subsequent points in a track; points may be discarded mid-track if the remainder constitutes a valid track. MAX_POINT_SPEED is the maximum implied speed (distance between points / time between points; in kph) to be permitted between subsequent points within a track. Points not meeting this threshold are dropped. Only one of --shorttrk or --fulltrk may be selected.')
    p.add_argument('--out_EPSG', type=int, default=4326, dest='OUT_EPSG', required=False, help='An EPSG number, indicating the output projection. Defaults to EPSG 4326 if omitted.')

    # Fetch the incoming arguments
    args = p.parse_args()

    # Retrieve the output directory, filename prefix, input filename and output EPSG.
    cmd_outdirectory = args.outdirectory
    cmd_out_filename_prefix = args.out_filename_prefix
    cmd_outputEPSG = args.OUT_EPSG

    # Establish the output sub-directories required for the process:
    split_file_foldername = "01_mmsi_split"
    segment_split_foldername = "02_segment_split"
    other_segment_split_subfoldername = "01_other_split"
    gis_output_foldername = "03_gis_output"

    # Build fully qualified directories for the sub-directories.
    cmd_split_file_directory = os.path.join(cmd_outdirectory, split_file_foldername)
    cmd_segment_split_directory = os.path.join(cmd_outdirectory, segment_split_foldername)
    cmd_segment_other_split_directory = os.path.join(cmd_outdirectory, segment_split_foldername, other_segment_split_subfoldername)
    cmd_gis_directory = os.path.join(cmd_outdirectory, gis_output_foldername)

    # If db based processing was selected, retrieve the tablename and connect string filename.
    if(not args.dbin is None):
        cmd_in_tablename = args.dbin[0]
        cmd_connect_string_filename = args.dbin[1]

        # If the specified input connection string file does not exist, abort and display an usage message.
        if not os.path.isfile(cmd_connect_string_filename):
            # Adjust to print function / python3 CH 20180107 (Add parens)
            print("Input connection string file (" + cmd_connect_string_filename + ") not found.\n")
            p.print_help()
            quit()

        # Extract a useable connection string from the provided file.
        conn_string = extract_pgpass_conn_string(cmd_connect_string_filename)
        if(conn_string == ""):
            
            print("Valid connection string not found in file: {}".format(cmd_connect_string_filename))
            p.print_help()
            quit()

    # Otherwise, if textfile-based processing was selected, retrieve the filename.
    else:
        
        cmd_in_filename = args.IN_FILENAME[0]
        
        # If the specified input data file does not exist, abort and display an usage message.
        if not os.path.isfile(cmd_in_filename):
            print("Input csv data file (" + cmd_connect_string_filename + ") not found.\n")
            p.print_help()
            quit()

    # If short track processing was selected, fetch the max elapsed time between points.
    if(not args.MAX_ELAPSED_TIME is None):
        cmd_max_elapsed_time = args.MAX_ELAPSED_TIME
        short_track_indicator = 1
        
    # Otherwise, long track processing was selected, fetch the track separation and max point speed values.
    else:
        cmd_track_separation_time = args.fulltrk[0]
        cmd_max_point_speed = args.fulltrk[1]
        short_track_indicator = 0
        
    # Load data into dataframe from file here.
    if(args.dbin is None):

        # Attempt to load the specified csv file into a dataframe, retrieving the 
        # fields required to generate tracks.
        source_dataframe = generate_dataframe_csv(cmd_in_filename)

        # If the loading failed, print an usage message and abort.
        if(source_dataframe is None):
            p.print_usage()
            quit()

    # Load data into dataframe from DB here.
    else:
        source_dataframe = generate_dataframe_pgdb(conn_string, cmd_in_tablename)

    # If the specified output directory already exists, abort and display an error message.
    if os.path.exists(cmd_outdirectory):
        # Adjust to print function / python3 CH 20180107 (Add parens)
        print("\nSpecified output directory already exists, aborting.")
        quit()
        
    else:

        # Attempt to create the output sub-directories required for the process.
        try:
            os.makedirs(cmd_outdirectory)
            os.makedirs(cmd_split_file_directory)
            os.makedirs(cmd_segment_split_directory)
            os.makedirs(cmd_segment_other_split_directory)
            os.makedirs(cmd_gis_directory)

        except:
            # Adjust to print function / python3 CH 20180107 (Add parens)
            print("\nError creating output directories, aborting.")
            quit()
        
    # Split the data table into separate text files based on mmsi.
    split_pre_tracks(source_dataframe, cmd_split_file_directory)
    
    # If short tracks are indicated, then proceed with the appropriate type 
    # of split on the per-mmsi files, then generate the GIS representations.
    if(short_track_indicator):
        
        # Adjust to print function / python3 CH 20180107 (Add parens)
        print("\nShort (point-to-point) track generation.")

        # Generate the dataframe representation of the segments.
        segment_array = generate_short_segments(source_dataframe, cmd_segment_split_directory, cmd_segment_other_split_directory, cmd_max_elapsed_time) 
        
        # Interpret the dataframe representation of the segments as a GIS polyline layers.
        generate_short_GIS(segment_array, cmd_gis_directory, cmd_out_filename_prefix, cmd_max_elapsed_time, cmd_outputEPSG)

        # Print out the speed threshold applied in processing.
        print("Speed bounds: " + str(min_speed_bound_kts) + " < speed in knots < " + str(max_speed_bound_kts))

    # If thresholded tracks are indicated, then proceed with the appropriate type 
    # of split on the per-mmsi files, then generate the GIS representations.
    else:
     
        # Print a message about the stage of processing
        # Adjust to print function / python3 CH 20180107 (Add parens)
        print("\nThresholded track generation.")

        # Generate the text representation of the tracks.
        segment_array = generate_threshold_tracks(source_dataframe, cmd_segment_split_directory, cmd_segment_other_split_directory, cmd_track_separation_time, cmd_max_point_speed)
    
        # Interpret the text representation of the tracks as GIS polyline and point shapefile layers.
        generate_threshold_GIS(segment_array, cmd_gis_directory, cmd_out_filename_prefix, cmd_outputEPSG)

# If we're invoked directly (which we generally expect), run.
if __name__ == "__main__":
    main()

                    
