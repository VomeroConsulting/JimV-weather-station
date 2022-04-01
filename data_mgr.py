from db_interface import MariaDatabase, WarningNetworkIssue
import logging


"""Data Manager is responsible for storing data to a database or
optionally in a json or CSV flat file format.
APP assumes data is always stored to a mariadb, but can be disabled
using mariadb=False."""


class DataMgr:
    def __init__(self, column_names, mariadb=True, **kwargs):
        self.column_names = column_names
        self.mariadb = mariadb

        # Supported command line KWARGS
        # Expecting file names to configure DB, flat file, or CSV data storage
        self.db_init = None
        self.flat_init = None
        self.csv_init = None

        # Local variables
        self.db_enabled = False
        self.flat_enabled = False
        self.csv_enabled = False

        # class variables
        self.data_mgr_db = None
        # data_entries is designed to be elastic store for sensor data.
        # Build as a FIFO, occupancy will only be greather than one if
        # a network problem is preventing data updates to MariaDB
        self.data_entries = []

        # valid KWARGS, '_init' values should be filenames
        # 'mariadb' = False to disable MariaDB usage
        for key, value in kwargs.items():
            if key == "mariadb":
                self.mariadb = value.strip()
            if key == "db_init":
                self.db_init = value.strip()
            if key == "flat_init":
                self.flat_init = value.strip()
            if key == "csv_init":
                self.csv_init = value.strip()

        # Initialize db and then try to open. open_db() function returns True if connected.
        # This is initial check of the db connection.
        # Must exit main program if fails initial try to open.
        if self.mariadb:
            try:
                self.data_mgr_db = MariaDatabase(self.column_names, self.db_init)
                self.db_enabled = self.data_mgr_db.open_db()
                logging.debug("MGR: Database open\n")

            except Exception as e:
                # Note: Must exit main if cannot connect on first attempt
                logging.error("Error opening MariaDB(): %s", e)
                raise

        if self.flat_init:
            # Creater Dictionary for write data
            self.data_dict = {}
            for key in self.column_names:
                self.data_dict[key] = None

            # ToDo Add code here

            self.flat_enabled = True

        if self.csv_init:
            # ToDo Add code here

            self.csv_enabled = True

    """Database already connected, need to get cursor and write data to DB"""

    def update_entry(self, data_entry):
        try:
            self.data_mgr_db.get_cursor_db()
            # if db.open_db():
            self.data_mgr_db.write_db(data_entry)
            logging.debug("MGR: Update Entry\n")
        except:
            raise ErrorCursor

    """
    # def update_entries(self, data_entry):
    def update_entries(self, params):
        self.data_entries.append(params)
        temp_data = self.data_entries

        if len(self.data_entries) == 10:
            logging.warning("Data Entries Queued = 10")
        try:
            # self.data_mgr_db.get_cursor_db()
            # if db.open_db():
            # self.data_mgr_db.write_db(data_entry)
            self.data_mgr_db.write_many_db(self.data_entries)
        except ErrorCursor:
            self.data_entries = temp_data
    """

    """
    ER_IPSOCK_ERROR Can't create IP socket
    ER_HOST_IS_BLOCKED Host '%s' is blocked because of many connection errors; unblock with 'mariadb-admin flush-hosts'
    ER_ABORTING_CONNECTION 
    ER_TOO_MANY_USER_CONNECTIONS
    ER_LOCK_WAIT_TIMEOUT
    """

    # def update_entries(self, data_entry):
    """update_entries takes a tuple of data as params.
    An FIFO is created containing tuples of data. In the event
    the DB command cannot be executed the data is queued and
    executed during next/future updates."""

    def update_entries(self, params):
        data_entry = ()
        # push data into FIFO
        self.data_entries.append(params)

        # May need to enhance worning in future, e.g. send email.
        if len(self.data_entries) > 1:
            logging.warning("MRG: Data Entries Queued = %s", len(self.data_entries))

        try:
            while len(self.data_entries) > 0:
                # pop FIFO to get oldest data and write to DB
                data_entry = self.data_entries.pop(0)
                logging.debug("MGR: Update Entries Before: %s\n", data_entry)
                self.data_mgr_db.write_one_db(data_entry)
                logging.debug("MGR: Update Entries After: %s\n", data_entry)

        except (WarningNetworkIssue):
            # rning If network is problem, Push data back into top of FIFO
            # Next update will contain multiple entries
            logging.debug("Network Issue, data entry not updated\n")
            logging.info("Network Issue, data entry not updated")
            self.data_entries.insert(0, data_entry)
            logging.debug("Network Issue, len data_entries = %s\n", len(self.data_entries))

        except Exception as e:
            # Non-Network error, will stop execution of main
            raise
