from flask import Flask, request
import sqlite3
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except Exception as ex:
        return ex.args
    conn.commit()
    return 'OK'

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id={trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_trips(data, conn):
    query = f""" INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_average_duration_subscriber_type(conn):
    query = f"""
        SELECT subscriber_type, SUM(duration_minutes)/count(1) average_duration 
        FROM trips 
        GROUP BY subscriber_type 
        ORDER BY average_duration DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_average_duration_subscribe_type(subscribe_type, conn):
    query = f"""SELECT subscriber_type, SUM(duration_minutes)/count(1) average_duration 
        FROM trips 
        WHERE subscriber_type='{subscribe_type}'
        GROUP BY subscriber_type
    """
    result = pd.read_sql_query(query, conn)
    return result 

def get_load_trips(data, conn):
    query = f""" SELECT start_station_id as totaltrips,
        start_station_name as station_name,start_time,duration_minutes as mean_duration_minutes 
        FROM trips t WHERE start_time LIKE '{data}%'"""
    wday = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    try:
        selected_data = pd.read_sql_query(query, conn, parse_dates='start_time')
        selected_data['dayofweek'] = selected_data['start_time'].dt.day_name()
        result = selected_data.groupby(['station_name','dayofweek']).agg({
            'totaltrips' : 'count', 
            'mean_duration_minutes' : 'mean'
        }).round(2).reindex(wday, level=1)
    except:
        return 'Error'
    return result.to_json()

##### route function #####

@app.route('/')
def home():
    return 'Hello World'

@app.route('/json',methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn=make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id,conn)
    return trip.to_json()

@app.route('/trips/add', methods=['POST'])
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration')
def route_all_average_duration_subscriber_type():
    conn=make_connection()
    trips = get_all_average_duration_subscriber_type(conn)
    return trips.to_json()

@app.route('/trips/average_duration/<subscribe_type>')
def route_average_duration_subscriber_type(subscribe_type):
    conn = make_connection()
    trip = get_average_duration_subscribe_type(subscribe_type,conn)
    return trip.to_json()

@app.route('/trips/load', methods=['POST'])
def route_load_trip():
    req = request.get_json(force=True)
    data = req['period']
    conn = make_connection()
    result = get_load_trips(data, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)