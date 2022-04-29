import os
import logging
import json

import mysql.connector

from errors import ErrorNetworkIssue


class MariaDatabase:
    """
    Maria database must be set up before running weather-station
    APP. See SQL sub-directory for setup. The set-up credentials
    includes and user, password and host. Defaults are
    'pi@localhost with "raspberry password
    """

    def __init__(self, column_names, db_config=None):

        self.column_names = column_names

        # Allows user to export OS variables,
        # otherwise generic defaults are used unless
        # overwrites are provided in json load file
        self.username = os.environ.get("WS_USERNAME", "pi")
        self.password = os.environ.get("WS_PASSWORD", "raspberry")
        self.host = os.environ.get("WS_HOST", "localhost")
        self.db_name = os.environ.get("WS_DB_NAME", "weather_data")
        self.db_table = os.environ.get("WS_DB_TABLE", "sensors")

        # Class specific variables
        self.connection = None
        self.cursor = None

        # ToDo Need to decide if failed_connections should throw error
        self.failed_connection = 0

        # Default variables can be overwritten via json file,
        # This section is ignored if json file does not exist
        if db_config:
            with open(db_config, "r") as f:
                credentials = json.load(f)

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

        # Set up Maria DB connection parameters
        self.connection_params = {
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.db_name,
            # "connect_timeout": 5,
        }

        # SQL command portion of DB write
        # if column_names list = [var1, var2] and table_name = sensors then
        # column_names_str = 'var1, var2' and
        # column_format_str = '%s, %s'
        # self.db_write_cmd = 'INSERT INTO sensors VALUES (var1, var2) (%s, %s)

        table_name_str = self.db_table
        column_names_str = ", ".join(self.column_names)
        column_format_str = ", ".join(["%s"] * len(self.column_names))

        self.db_write_cmd = (
            "INSERT INTO "
            + table_name_str
            + " ("
            + column_names_str
            + ") VALUES ("
            + column_format_str
            + ")"
        )

        # SQL read commenad is used only for testing this module,
        # Weather Station only writes to DB
        self.db_read_cmd = os.environ.get("WS_DB_READ_CMD")
        if self.db_read_cmd == None:
            self.db_read_cmd = "SELECT * FROM {0} ORDER BY id DESC LIMIT 5".format(
                self.db_table
            )

    """
    Opens DB connection and connects cursor. In the event of a network issue,
    a ErrorNetworkIssue is thrown.
    """

    def open_db(self):
        try:
            logging.info("DB-IF: Try connect()")
            self.connection = mysql.connector.connect(**self.connection_params)
            logging.info("DB-IF: Try cursor()")
            self.cursor = self.connection.cursor()
        except (
            mysql.connector.errors.OperationalError,
            mysql.connector.errors.InterfaceError,
        ) as e:
            logging.info("DB-IF: Error OperationError or InterfaceError")
            logging.info("DB-IF: Thrown %s", e)
            raise ErrorNetworkIssue
        except Exception as e:
            logging.error("DB-IF: Error NOT OperationError or InterfaceError")
            logging.error("DB-IF: Thrown %s", e)
            raise e

    """
    send_data requires connection to DB before calling this function.
    Also, routine does not close connection.
    """

    def send_data(self, entry, multi_entry=False):
        # Write to database command the same for all INSERT INTO TABLE
        cmd = self.db_write_cmd

        if multi_entry:
            if type(entry) is tuple:
                entries = [entry]
            elif type(entry) is list:
                entries = entry
            else:
                # Data must be tuple or list of tuples
                exit(1)

        # Assumes connection is open and good
        try:
            if multi_entry:
                logging.info("DB-IF: Try executemany()")
                self.cursor.executemany(cmd, entries)
            else:
                logging.info("DB-IF: Try execute()")
                self.cursor.execute(cmd, entry)
            logging.info("DB-IF: Try comit()")
            self.connection.commit()

        except (mysql.connector.OperationalError, mysql.connector.InterfaceError) as e:
            logging.info("DB-IF: Error OperationError or InterfaceError")
            logging.info("DB-IF: Thrown %s", e)
            logging.info("DB-IF: Try rollback()")
            self.connection.rollback()
            raise ErrorNetworkIssue
        except Exception as e:
            logging.error("DB-IF: Error NOT OperationError or InterfaceError")
            logging.info("DB-IF: Thrown %s", e)
            logging.info("DB-IF: Try rollback()")
            self.connection.rollback()
            raise e

    # Closes cursor and connection
    def close_db(self):
        logging.info("DB-IF: Try cursor.close()")
        self.cursor.close()
        logging.info("DB-IF: Try connection.close()")
        self.connection.close()

    def is_connected_db(self):
        logging.info("DB-IF: Try connection.is_connected()")
        return self.connection.is_connected()

    # Following code not rigorously verified, use at own risk.
    ##########
    # The functions below are not used by the weather station app, but
    # were provided for debug and test.
    ##########

    """
    Opens DB connection to host, errors need to be managed by calling function.
    """

    def open_connection_db(self):
        self.connection = mysql.connector.connect(**self.connection_params)
        return self.connection.is_connected()

    """
    Closes DB connection to host, errors need to be managed by calling function.
    """

    def close_connection_db(self):
        self.connection.close()

    """
    Opens DB cursor to host, errors need to be managed by calling function.
    """

    def open_cursor_db(self):
        self.cursor = self.connection.cursor()

    """
    Closes DB cursor to host, errors need to be managed by calling function.
    """

    def close_cursor_db(self):
        self.cursor.close()

    """
    Checks if database is connected.
    """

    """
    Writes one row to DB. User must manage errors.
    User must open connection and cursor.
    """

    def write_one_db(self, params):
        try:
            cmd = self.db_write_cmd
            data = params

            self.cursor.execute(cmd, data)
            self.connection.commit()

        except Exceptions as e:
            logging.warning(f"Error Exception in write_one_db(): {e}")
            self.connection.rollback()
            raise e

    """
    Writes multiple rows to DB. User must manage errors.
    User must open connection and cursor.
    """

    def write_many_db(self, params):
        try:
            cmd = self.db_write_cmd
            data = params

            self.cursor.executemany(cmd, data)
            self.connection.commit()

        except Exceptions as e:
            logging.info(f"Error Exception in write_one_db(): {e}")
            self.connection.rollback()
            raise e

    # Read DB is only used for testing this module
    def read_db(self):
        statement = self.db_read_cmd
        self.cursor.execute(statement)
        return self.cursor.fetchall()


if __name__ == "__main__":
    from datetime import datetime
    import time

    """User must creater list of column names that matches one's own implemenation"""
    column_names = [
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

    # Open DB and cursor
    # db = MariaDatabase(column_names, db_config="json_backend_private.load")
    db = MariaDatabase(column_names, "json_backend_private.load")
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

    db.send_data(tuple(params))

    # modify some of the data and update DB
    time.sleep(1)
    params[0] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params[1] += 7
    params[2] += 7
    params[3] = "SSE"

    db.send_data(tuple(params))
    db.close_db()

    db_fe = MariaDatabase(column_names, "json_frontend_private.load")
    db_fe.open_db()

    # Read in some of the data
    read_data = db_fe.read_db()
    for x in read_data:
        print(x)

    db_fe.close_db
