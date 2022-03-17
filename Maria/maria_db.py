import os
import mysql.connector
import logging


class maria_database:
    def __init__(self):
        """Maria database must be set up before running weather-station
        APP. See SQL sub-directory for setup. The set-up credentials
        includes and user, password and host. Defaults are
        'pi@localhost with "raspberry password"""

        # Initialization of Class variables
        self.username = "pi"
        self.password = "raspberry"
        self.host = "localhost"
        self.db_name = "weather_data"
        self.db_table = "sensors"

        # Set up connection parameters to database
        self.connection_params = {
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.db_name,
        }
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
        self.db_read_cmd = "SELECT * FROM {0} ORDER BY id DESC LIMIT 5".format(
            self.db_table
        )

    # Opens DB connection and connects cursor
    def open_db(self):
        self.connection = mysql.connector.connect(**self.connection_params)
        self.cursor = self.connection.cursor()

    # Writes one row to DB
    def write_db(self, params):
        try:
            cmd = self.db_write_cmd
            data = tuple(params)
            self.cursor.execute(cmd, data)
            self.connection.commit()

        except self.connector.Error as e:
            logging.warning(f"Error Exception in write_db(): {e}")
            self.connection.rollback()
            raise

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
    db = maria_database()
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

    # Read in some of the data
    read_data = db.read_db()
    for x in read_data:
        print(x)

    db.close_db
