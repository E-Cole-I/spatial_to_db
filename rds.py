# Database Information pulled from external file
def json_config(file_path):
    import json
    with open(file_path) as json_data_file:
        data = json.load(json_data_file)
    return data

def setup_db():
    import psycopg2
    # Set up variables for configuration
    file_path = '../config/config.json'
    config = json_config(file_path)
    dbname = config['redshift']['dbname']
    host = config['redshift']['host']
    port = config['redshift']['port']
    user = config['redshift']['user']
    password = config['redshift']['password']
    
    # Set up database connection
    con = psycopg2.connect(dbname=dbname,
                       host= host,
                       port=port, 
                       user=user, 
                       password=password)
    return con
    


def input_query(query):
 
    con = setup_db()
    
    # Establish the cursor
    cur=con.cursor()
    
    # Execute the query
    cur.execute(query)
    
    
    # Commit the changes
    con.commit()
    



def output_query(query):
    
    con = setup_db()
    
    # Establish the cursor
    cur=con.cursor()
    
    # Execute the query
    cur.execute(query)
    
    
    # Fetch the data return it
    data = cur.fetchall()
    cur.close()
    return data

