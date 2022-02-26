import os
import mysql.connector as database

# import MySQLdb, datetime, http.client, json, os
# import io
# import gzip


class maria_database:
    def __init__(self):
        """Maria database must be set up before running weather-station
        APP. See SQL sub-directory for setup. The set-up credentials
        includes and user, password and host. Defaults are
        'pi@localhost with "raspberry password"""
        # Allows user to export OS variables for user/password
        self.username = os.environ.get("WS_USERNAME")
        self.password = os.environ.get("WS_PASSWORD")
        self.host = os.environ.get("WS_HOST")
        if self.username == None or self.passwword == None or self.host == None:
            # Easy default user used
            self.username = "pi"
            self.password = "raspberry"
            self.host = "localhost"

        """Default database name and tables can be overwritten.
        Default database is "weather_data' and table is "sensors"."""
        self.db_name = os.environ.get("WS_DB_NAME")
        if self.db_name == None:
            self.db_name = "weather_data"

        self.db_table = os.environ.get("WS_TABLE")
        if self.db_table == None:
            self.db_table = "sensors"

        # Set up connection to database
        self.connection_params = {
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.db_name,
            "auth_plugin": "mysql_native_password",
        }

    def open_db(self):
        self.connection = database.connect(**self.connection_params)
        # self.cursor = self.connection.cursor()
        # try:
        # self.connection = database.connect(**self.connection_params)
        # self.cursor = self.connection.cursor()
        # except Exception as e:
        # Send exception back to main()
        # logging.debug(f"Error initializiong database connection/cursor: {e}")
        # raise

    def write_db(self, params):
        # Replace any 'None' with NULL in db
        for i in range(len(params)):
            if params[i] == "None":
                params[i] = "NULL"
        for x in params:
            print(x)


#     def execute(self, query, params=[]):
#         try:
#             self.cursor.execute(query, params)
#             self.connection.commit()
#         except:
#             self.connection.rollback()
#             raise

#     def query(self, query):
#         cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
#         cursor.execute(query)
#         return cursor.fetchall()

#     def __del__(self):
#         self.connection.close()


# class oracle_apex_database:
#     def __init__(self, path, host="apex.oracle.com"):
#         self.host = host
#         self.path = path
#         # self.conn = httplib.HTTPSConnection(self.host)
#         self.conn = http.client.HTTPSConnection(self.host)
#         self.credentials = None
#         credentials_file = os.path.join(os.path.dirname(__file__), "credentials.oracle")

#         if os.path.isfile(credentials_file):
#             f = open(credentials_file, "r")
#             self.credentials = json.load(f)
#             f.close()
#             for key, value in self.credentials.items():  # remove whitespace
#                 self.credentials[key] = value.strip()
#         else:
#             print("Credentials file not found")

#         self.default_data = {"Content-type": "text/plain", "Accept": "text/plain"}

#     def upload(
#         self,
#         id,
#         ambient_temperature,
#         ground_temperature,
#         air_quality,
#         air_pressure,
#         humidity,
#         wind_direction,
#         wind_speed,
#         wind_gust_speed,
#         rainfall,
#         created,
#     ):
#         # keys must follow the names expected by the Orcale Apex REST service
#         oracle_data = {
#             "LOCAL_ID": str(id),
#             "AMB_TEMP": str(ambient_temperature),
#             "GND_TEMP": str(ground_temperature),
#             "AIR_QUALITY": str(air_quality),
#             "AIR_PRESSURE": str(air_pressure),
#             "HUMIDITY": str(humidity),
#             "WIND_DIRECTION": str(wind_direction),
#             "WIND_SPEED": str(wind_speed),
#             "WIND_GUST_SPEED": str(wind_gust_speed),
#             "RAINFALL": str(rainfall),
#             "READING_TIMESTAMP": str(created),
#         }

#         for key in oracle_data.keys():
#             if oracle_data[key] == str(None):
#                 del oracle_data[key]

#         return self.https_post(oracle_data)

#     def https_post(self, data, attempts=3):
#         attempt = 0
#         headers = self.default_data.copy()
#         headers.update(self.credentials)
#         headers.update(data)

#         # headers = dict(self.default_data.items() + self.credentials.items() + data.items())
#         success = False
#         response_data = None

#         while not success and attempt < attempts:
#             try:
#                 self.conn.request("POST", self.path, None, headers)
#                 response = self.conn.getresponse()
#                 response_data = response.read()
#                 print(
#                     "Response status: %s, Response reason: %s, Response data: %s"
#                     % (response.status, response.reason, response_data)
#                 )
#                 success = response.status == 200 or response.status == 201
#             except Exception as e:
#                 print("Unexpected error", e)
#             finally:
#                 attempt += 1

#         return response_data if success else None

#     def __del__(self):
#         self.conn.close()


# class weather_database:
#     def __init__(self):
#         self.db = mysql_database()
#         self.insert_template = "INSERT INTO WEATHER_MEASUREMENT (AMBIENT_TEMPERATURE, GROUND_TEMPERATURE, AIR_QUALITY, AIR_PRESSURE, HUMIDITY, WIND_DIRECTION, WIND_SPEED, WIND_GUST_SPEED, RAINFALL, CREATED) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
#         self.update_template = (
#             "UPDATE WEATHER_MEASUREMENT SET REMOTE_ID=%s WHERE ID=%s;"
#         )
#         self.upload_select_template = (
#             "SELECT * FROM WEATHER_MEASUREMENT WHERE REMOTE_ID IS NULL;"
#         )

#     def is_number(self, s):
#         try:
#             float(s)
#             return True
#         except ValueError:
#             return False

#     def is_none(self, val):
#         return val if val != None else "NULL"

#     def insert(
#         self,
#         ambient_temperature,
#         ground_temperature,
#         air_quality,
#         air_pressure,
#         humidity,
#         wind_direction,
#         wind_speed,
#         wind_gust_speed,
#         rainfall,
#         created=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#     ):
#         params = (
#             ambient_temperature,
#             ground_temperature,
#             air_quality,
#             air_pressure,
#             humidity,
#             wind_direction,
#             wind_speed,
#             wind_gust_speed,
#             rainfall,
#             created,
#         )
#         print(self.insert_template % params)
#         self.db.execute(self.insert_template, params)

#     def upload(self):
#         results = self.db.query(self.upload_select_template)

#         rows_count = len(results)
#         if rows_count > 0:
#             print("%d rows to send..." % rows_count)
#             odb = oracle_apex_database(
#                 path="/pls/apex/raspberrypi/weatherstation/submitmeasurement"
#             )

#             if odb.credentials == None:
#                 return  # cannot upload

#             for row in results:
#                 response_data = odb.upload(
#                     row["ID"],
#                     row["AMBIENT_TEMPERATURE"],
#                     row["GROUND_TEMPERATURE"],
#                     row["AIR_QUALITY"],
#                     row["AIR_PRESSURE"],
#                     row["HUMIDITY"],
#                     row["WIND_DIRECTION"],
#                     row["WIND_SPEED"],
#                     row["WIND_GUST_SPEED"],
#                     row["RAINFALL"],
#                     row["CREATED"].strftime("%Y-%m-%dT%H:%M:%S"),
#                 )

#                 if response_data != None and response_data != "-1":
#                     json_dict = json.loads(
#                         gunzip_bytes(response_data)
#                     )  # 2019 post-apex upgrade change
#                     # json_dict = json.loads(response_data.decode()) # Python3 change
#                     oracle_id = json_dict["ORCL_RECORD_ID"]
#                     if self.is_number(oracle_id):
#                         local_id = str(row["ID"])
#                         self.db.execute(self.update_template, (oracle_id, local_id))
#                         print(
#                             "ID: %s updated with REMOTE_ID = %s" % (local_id, oracle_id)
#                         )
#                 else:
#                     print("Bad response from Oracle")
#         else:
#             print("Nothing to upload")

if __name__ == "__main__":
    from datetime import datetime

    db = maria_database()
    db.open_db()
    params = []
    params.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    params.append(1.1)
    params.append(2.2)
    params.append("NNW")
    params.extend([3.3, 4.4, 5.5])
    # params.append(3.3)
    # params.append(4.4)
    # params.append(5.5)
    params.append(6.6)
    params.append(7.7)

    db.write_db(params)
