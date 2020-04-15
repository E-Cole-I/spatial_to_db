
import rds
import fiona
import re
import random
import time
from shapely.geometry import shape, Polygon, MultiPolygon



def geom_finder(file_item, centroid_list):

    # Nab coordinates from the shapefile
    gtype = file_item['geometry']['type']
    
    # Check type of geometry (Important for handling of mixed geometries)
    if gtype == 'Polygon':
        # Grab coordinates from the object
        coords = file_item['geometry']['coordinates'][0]
        
        # Convert them to a shapely object
        shp_geom = Polygon(coords)
        
        # Get the WKT coordinates from the geometry (Suitable for redshift/postgres)
        wkt_geom = shp_geom.wkt
        
        # Grab centroid to run contains query for testing. 
        poly_centroid = shp_geom.centroid.wkt
        centroid_list.append(poly_centroid)
    
    # If the geometry type is multipolygon
    elif gtype == 'MultiPolygon':
        
        # Coordinates for a multipolygon can be converted without the 0 index
        coords = file_item['geometry']['coordinates']
        gtype = file_item['geometry']['type']
        
        # Shapely Geometry type
        wkt_geom = MultiPolygon(shape(file_item['geometry'])).wkt
 
        
    else:
        # Catch any weird/not appropriate geometry types
        print("Not a Polygon or a Multipolygon", "\nGeometry Type", gtype)
        pass
    
    return wkt_geom   

# This is for testing purposes......

def test_shp(table_name, centroid_list):
    try:
        # Test 10 random centroid points to make sure polygons have valid shapes
        for i in range(10):
            point = random.choice(centroid_list)

            query_select = """ select * from {} where ST_Contains({}.geom, ST_GeomFromText('{}'))

                            """.format(table_name, table_name, point)
            test_query = rds.output_query(query_select)

        print('Success!!! Found all 10 points..')
    except:
        print('The selected point did not return any polygons... Something may be wrong')




def shp_to_db(file_name, del_table='N', table_name = 'default'):
    """
    This script is meant to take a shapefile, create a db table in postgres/redshift, and populate it. 
    
    Right now, this script only works with polygon/multipolygon shapefiles... This will change soon.
    """
            
        
    start_time = time.time()    
        
    # Open the file
    file = fiona.open(file_name)
    
    #--------------------START OF TABLE CREATION----------------------------------
    
    
    # Table name grabbed from the file if the argument is the default
    if table_name == 'default':
        table_name = file_name.split('/')[-1].split('.')[-2]
    print('Creating.....', table_name, "table")
    
    if del_table == 'Y':
        # This mostly for testing, but it gives the user the option to delete an existing table.
        try:
            delete_query = 'DROP TABLE {};'.format(table_name)
            rds.input_query(delete_query)

        except:
            print('Could Not Delete Table')
            
            # Stop code if the table couldn't be deleted and the user selected Y
            return
    
    
    # Get the table columns and data types

    # Gets the keys of the fiona object which is essentially a dictionary
    column_list = list(file[0]['properties'].keys())


    # Start building the query to create the table
    base_query = "CREATE TABLE " + table_name + " ( "
    idx = 0
    
    # Loop through the column list to add new coluns to query string..
    for i in column_list:
        col = i
        col_type = type(file[0]['properties'][i])
        col_type_pgsql = None

        # Checking the column datatype to adjust it to a postgres type. I'm just doing basic conversions, but a more detailed approach is pretty simple. 
        if col_type == str:
            col_type_pgsql = 'TEXT'

        elif col_type == int:
            col_type_pgsql = 'BIGINT'
            
        elif col_type == float:
            col_type_pgsql = 'DOUBLE PRECISION'

        # Catch all bin... 
        else:
            col_type_pgsql = 'TEXT'

        base_query += col + " " + col_type_pgsql + ", "


    # Add the final bracket and colon on the query string
    base_query += "geom geometry);"


    # Execute the query 
    rds.input_query(base_query)
    
    #--------------------END OF TABLE CREATION----------------------------------
    
    # Build the insert query so the data can be transfered
    centroid_list = []
    
   
    
    
    for j in file:
        
        
        # Status Update for Larger Files. 
        idx+=1 
        if idx%100 == 0:
            print('Done with', idx,'records....')
            
            
        # Insert query string
        insert_query = "INSERT INTO " + table_name + " ( "
        inputs = ""


        # Loop over the shapefile columns and add them to the string
        for i in column_list:
            insert_query += i + ", "
            
            
            
            # Creating the input/values part of the insert query
            input_string = str(j['properties'][i]).replace("'","")
            inputs += "'" + input_string + "', "

        # Finish with the correct version of the geometry transform (No Coordinate system.)
        insert_query += ' geom) values ( ' + inputs + "ST_GeomFromText('{}'));"

        # Get the wkt geometry 
        geom_col = geom_finder(j, centroid_list)
        
        
        
        # Add the geometry to the string through string formatting
        insert_query = insert_query.format(geom_col)


        # Run the insert query
        rds.input_query(insert_query)
        
    print('Done Creating the Table......')
    end_time = time.time()
    
    print("Shape to Table Time: ", (end_time-start_time)/60, "minutes")
    
    print('Starting Testing...')
    test_shp(table_name, centroid_list)
    end_time = time.time()
    print("Shape to Table Time With Testing: ", (end_time-start_time)/60, "minutes")
    
    

# TEST - County shapefile
# file_name = '../Data/tl_2019_us_county.shp'
# arguments = shp_to_db(file_name, 'Y')    
    






