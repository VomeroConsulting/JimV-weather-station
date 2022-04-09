import os
import logging
import json


class FlatDatabase:
    """ """

    def __init__(self, column_names, flat_config):

        self.field_names = column_names
        self.flat_file_name = flat_config

        # csv uses \r\n as line seperator, below code uses OS line terminator
        self.eol = os.linesep
        self.file_handle = None

        # ToDo Need to decide if failed_connections should throw error
        self.failed_connection = 0

        # Check if file exists
        if os.path.exists(self.flat_file_name):

            # Check that file can be written
            with open(self.flat_file_name, "a") as self.file_handle:
                pass

        # File does not exist, create file and write header
        else:
            with open(self.flat_file_name, "a", newline=self.eol) as self.file_handle:
                pass

    """
    Opens DB connection and connects cursor. In the event of a network issue,
    a ErrorNetworkIssue is thrown.
    """

    def open_db(self):
        # try:
        self.file_handle = open(self.flat_file_name, "a")

    """
    send_data requires connection to DB before calling this function.
    Also, routine does not close connection.
    """

    def send_data(self, entry, multi_entry=False):

        self.dict_entry = {}
        for count, key in enumerate(self.field_names):
            self.dict_entry[key] = entry[count]

        # if multi_entry:
        #     if type(entry) is tuple:
        #         entries = [entry]
        #     elif type(entry) is list:
        #         entries = entry
        #     else:
        #         # Data must be tuple or list of tuples
        #         exit(1)

        # Assumes connection is open and good
        # try:
        #     if multi_entry:
        #         self.flat_writer.writerow(entry)
        #     else:
        json.dump(self.dict_entry, self.file_handle)
        self.file_handle.write(self.eol)
        # except (mysql.connector.OperationalError, mysql.connector.InterfaceError) as e:
        #     logging.debug(f"Warming Exception in write_one_db() Operational Error: {e}")
        #     logging.warn(f"Warning Exception in write_one_db() Operational Error: {e}")
        #     self.connection.rollback()
        #     raise ErrorNetworkIssue
        # except Exception as e:
        #      raise e

    # Closes cursor and connection
    def close_db(self):
        self.file_handle.close()


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
    flat_db = CsvDatabase(column_names, "/tmp/test_dict_header.csv")

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

    flat_db.open_db()
    flat_db.send_data(params)
    flat_db.close_db()

    # modify some of the data and update DB
    time.sleep(1)
    params[0] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params[1] += 7
    params[2] += 7
    params[3] = "SSE"

    flat_db.open_db()
    flat_db.send_data(params)
    flat_db.close_db()

    # db_fe = MariaDatabase(column_names, "json_frontend_private.load")
    # db_fe.open_db()

    # Read in some of the data
    # read_data = db_fe.read_db()
    # for x in read_data:
    # print(x)

    # db_fe.close_db
