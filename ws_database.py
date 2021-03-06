from multiprocessing import connection
import os
import mysql.connector
import logging
import json


class ErrorConnector(Exception):
    """Raised when error connecting to mysql"""

    pass


class ErrorCursor(Exception):
    """Raised when error with mysql cursor"""

    pass


class MariaDatabase:
    def __init__(self, arg=None):
        """Maria database must be set up before running weather-station
        APP. See SQL sub-directory for setup. The set-up credentials
        includes and user, password and host. Defaults are
        'pi@localhost with "raspberry password"""

        # Allow for json file to be passed as argument
        if not arg:
            arg = "json_backend.load"
        json_file = arg

        # Allows user to export OS variables, otherwise pick generic defaults
        self.username = os.environ.get("WS_USERNAME", "pi")
        self.password = os.environ.get("WS_PASSWORD", "raspberry")
        self.host = os.environ.get("WS_HOST", "localhost")
        self.db_name = os.environ.get("WS_DB_NAME", "weather_data")
        self.db_table = os.environ.get("WS_DB_TABLE", "sensors")
        # self.db_write_fields = os.environ.get("WS_DB_WRITE_FIELDS")
        # self.db_write_cmd = os.environ.get("WS_DB_WRITE_CMD")
        # self.db_read_cmd = os.environ.get("WS_DB_READ_CMD")

        self.connection = None
        self.cursor = None

        self.failed_connection = 0

        # variables can be overwritten via json file, ignore if json file does not exist
        try:
            f = open(json_file, "r")
            credentials = json.load(f)
            f.close()

            for key, value in credentials.items():
                credentials[key] = value.strip()
                if key == "WS_USERNAME":
                    self.username = value
                if key == "WS_PASSWORD":
                    self.password = value
                if key == "WS_HOST":
                    self.host = value
                if key == "WS_DB_NAME":
                    self.db_name = value
                if key == "WS_DB_TABLE":
                    self.db_table = value

        except:
            logging.info(f"JSON file {json_file} not found")

        # Set up connection parameters to database
        self.connection_params = {
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.db_name,
        }

        """ User must change db fields based on own implementation.
        This section of code must be modified based on user implementation."""
        # Defaults for database read/write can be configured as env variables
        self.db_write_fields = [
            "time",
            "ws_ave",
            "ws_max",
            "w_dir",
            "humid",
            "press",
            "temp",
            "therm",
            "rain",
        ]

        # SQL command portion of DB write
        self.db_write_cmd = (
            "INSERT INTO {0} ({1}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                self.db_table, ", ".join(self.db_write_fields)
            )
        )

        # SQL read commenad is used only for testing this module,
        # Weather Station only writes to DB
        self.db_read_cmd = os.environ.get("WS_DB_READ_CMD")
        if self.db_read_cmd == None:
            self.db_read_cmd = "SELECT * FROM {0} ORDER BY id DESC LIMIT 5".format(
                self.db_table
            )

    # Opens DB connection and connects cursor
    def open_db(self):
        """Opens DB connection to host, any error should be caught by calling function"""
        self.connection = mysql.connector.connect(**self.connection_params)
        # self.cursor = self.connection.cursor()
        # return self.connection.is_connected()

    # Connect to DB
    def connect_db(self):
        self.connection = mysql.connector.connect(**self.connection_params)
        return self.connection.is_connected()

    """
    # Checks is connected and connects cursor
    def get_cursor_db(self):
        try:
            self.connection.ping(reconnect=True, attempts=3, delay=2)

        except Error as e:
            # InterfaceError
            self.connection.close_db()
            if self.connect_db()
                self.failed_connection += 1
                return False

        finally:
            self.cursor = self.connection.cursor()
    """

    # Opens DB connection and connects cursor
    def get_cursor_db(self):
        self.connection.ping(reconnect=True, attempts=3, delay=1)
        self.cursor = self.connection.cursor()

    # Writes one row to DB
    def write_db(self, params):
        # Make sure connection is still good
        try:
            self.connection.ping(reconnect=True, attempts=3, delay=1)
            # self.connection.close()

            self.cursor = self.connection.cursor()

            cmd = self.db_write_cmd
            data = params
            # data = tuple(params.values())
            self.cursor.execute(cmd, data)
            self.connection.commit()
            self.cursor.close()

        except self.connection.Error as e:
            logging.warning(f"Error Exception in write_db(): {e}")
            self.connection.rollback()
            raise

    # Writes one row to DB
    def write_one_db(self, entry):
        # Make sure connection is still good
        try:
            self.connection.ping(reconnect=True, attempts=3, delay=1)
            # self.connection.close()

            try:
                self.cursor = self.connection.cursor()
                cmd = self.db_write_cmd
                self.cursor.execute(cmd, entry)
                self.connection.commit()
                self.cursor.close()

            except self.connection.Error as e:
                logging.warning(f"Error Exception in write_many_db(): {e}")
                self.connection.rollback()
                raise ErrorCursor
        except:
            raise ErrorConnector

    # Writes multiple rows to DB
    # Need to consider error case when connection is temporaly lost,
    # can queue multiple rows and send when connection restores.
    def write_many_db(self, entries):
        # Make sure connection is still good
        try:
            self.connection.ping(reconnect=True, attempts=3, delay=1)
            # self.connection.close()

            try:
                self.cursor = self.connection.cursor()
                # temp_data = entries

                cmd = self.db_write_cmd

                # while len(entries) > 0:
                # row = entries.pop(0)
                # data = entries.pop(0)
                # self.cursor.execute(cmd, data)
                self.cursor.executemany(cmd, entries)

                self.connection.commit()
                self.cursor.close()

            except self.connection.Error as e:
                logging.warning(f"Error Exception in write_many_db(): {e}")
                self.connection.rollback()
                entries = temp_data
                raise ErrorCursor
        except:
            raise ErrorConnector

    """
    def write_db(self, params):
        try:
            self.cursor = self.connection.cursor()

            cmd = self.db_write_cmd
            # data = tuple(params)
            data = tuple(params.values())
            self.cursor.execute(cmd, data)
            self.connection.commit()
            self.cursor.close()

        except self.connection.Error as e:
            logging.warning(f"Error Exception in write_db(): {e}")
            self.connection.rollback()
            raise

    """
    # Read DB is only used for testing this module
    def read_db(self):
        statement = self.db_read_cmd
        self.cursor.execute(statement)
        return self.cursor.fetchall()

    # Closes cursor and connection if opened
    def close_db(self):
        if self.connection.is_connected():
            self.cursor.close()
            self.connection.close()


if __name__ == "__main__":
    from datetime import datetime
    import time

    # Open DB and cursor
    db = MariaDatabase("json_backend_private.load")
    db.open_db()

    # Weather Station data
    params = []
    params.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    params.append(1.1)
    params.append(2.2)
    params.append("NNW")
    params.append(3.3)
    params.append(None)
    params.append(5.5)
    params.append(6.6)
    params.append(7.7)

    db.write_db(params)

    # modify some of the data and update DB
    time.sleep(1)
    params[0] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params[1] += 7
    params[2] += 7
    params[3] = "SSE"

    db.write_db(params)
    db.close_db()

    db_fe = MariaDatabase("json_frontend_private.load")
    db_fe.open_db()

    # Read in some of the data
    read_data = db_fe.read_db()
    for x in read_data:
        print(x)

    db_fe.close_db
