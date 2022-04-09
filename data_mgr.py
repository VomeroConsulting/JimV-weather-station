from db_interface import MariaDatabase, ErrorNetworkIssue
import logging
import os
import flat_interface
import csv_interface


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
        self.db_config = None
        self.flat_config = None
        self.csv_config = None

        # Local variables
        self.db_enabled = False
        self.flat_enabled = False
        self.csv_enabled = False

        # class variables
        self.data_mgr_db = None
        self.data_mgr_csv = None
        self.data_mgr_flat = None

        # data_entries is designed to be elastic store for sensor data.
        # Build as a FIFO, occupancy will only be greather than one if
        # a network problem is preventing data updates to MariaDB
        self.data_entries_db = []
        self.data_entries_flat = []
        self.data_entries_csv = []

        self.flat_file_path = None
        self.flat_file_name = None
        self.csv_file_path = None
        self.csv_file_name = None

        # valid KWARGS, '_config' values should be filenames
        # 'mariadb' = False to disable MariaDB usage
        for key, value in kwargs.items():
            if key == "mariadb":
                self.mariadb = False if (value == False or value == "False") else True
            if key == "db_config":
                self.db_config = (
                    None if (value == None or value == "None") else value.strip()
                )
            if key == "flat_config":
                self.flat_config = value.strip()
            if key == "csv_config":
                self.csv_config = value.strip()

        # Initialize db and then try to open. open_db() function returns True if connected.
        # This is initial check of the db connection.
        # Must exit main program if fails initial try to open.
        if self.mariadb:
            try:
                logging.debug("MGR: Initial Connection to Database")
                self.data_mgr_db = MariaDatabase(self.column_names, self.db_config)
                self.data_mgr_db.open_db()
                self.db_enabled = self.data_mgr_db.is_connected_db()
                self.data_mgr_db.close_db()
                logging.debug("MGR: PASS Initial Connection to Database\n")

            except Exception as e:
                # Note: Must exit main if cannot connect on first attempt
                logging.error("Error opening MariaDB first time: %s", e)
                raise

        if self.flat_config:
            self.data_mgr_flat = flat_interface.FlatDatabase(
                self.column_names, self.flat_config
            )
            self.flat_enabled = True

        if self.csv_config:
            self.data_mgr_csv = csv_interface.CsvDatabase(column_names, self.csv_config)
            self.csv_enabled = True

    """update_entries takes a tuple of data as params.
    A ErrorNetworkIssue is thrown if entry cannot be written to DB, user must
    manage errors."""

    def update_entry(self, params):
        # First check to see if DB can be re-opened, Allows Network Issue to
        # be identified and managed by calling program
        try:
            self.data_mgr_db.open_db()

            try:
                # Write to DB
                self.data_mgr_db.write_one_db(params)

            except (ErrorNetworkIssue):
                logging.debug("Network Issue, data entry not updated")
                raise ErrorNetworkIssue

            except Exception as e:
                # Other error, stop execution of program
                logging.error("update entry(), data entry not updated: %s", e)
                raise

        except ErrorNetworkIssue as e:
            # Keep processing data if network goes down
            raise ErrorNetworkIssue

        except Exception as e:
            # Non-Network error, will stop execution of main
            raise

        finally:
            # Close upon exit
            self.data_mgr_db.close_db()

    def update_entries(self, params):
        if self.db_enabled:
            self._send2db(params)

        if self.flat_enabled:
            self._send2flat(params)

        if self.csv_enabled:
            self._send2csv(params)

    """update_entries takes a tuple of data as params.
    An FIFO is created containing tuples of data. In the event
    the DB command cannot be executed the data is queued and
    executed during next/future updates.
    Network errors are managed by this routine and will not stop execution of main."""

    def _send2db(self, params):
        data_entry = ()
        # push data into FIFO
        self.data_entries_db.append(params)

        # See if DB can be re-opened, Allow Network Issue to pass
        try:
            self.data_mgr_db.open_db()

            # May need to enhance worning in future, e.g. send email.
            if len(self.data_entries_db) > 1:
                logging.warn("MRG: Data Entries Queued = %s", len(self.data_entries_db))

            try:
                while len(self.data_entries_db) > 0:
                    # pop FIFO to get oldest data and write to DB
                    data_entry = self.data_entries_db.pop(0)
                    logging.debug("MGR: Update Entry")
                    self.data_mgr_db.send_data(data_entry)

            except (ErrorNetworkIssue):
                # If network is problem, Push entry back into top of FIFO
                # Next update will contain multiple entries
                self.data_entries_db.insert(0, data_entry)
                logging.debug("Network Issue, data entry not updated")
                logging.warn("Network Issue, data entry not updated")
                pass

            except Exception as e:
                # Other error, stop execution of program
                logging.error("update entry(), data entry not updated: %s", e)
                raise

            finally:
                # Close after writing all entries
                self.data_mgr_db.close_db()

        except ErrorNetworkIssue as e:
            # Keep processing data if network goes down
            pass

        except Exception as e:
            # Non-Network error, will stop execution of main
            raise

        finally:
            # Close upon exit
            self.data_mgr_db.close_db()

    def _send2flat(self, params):
        self.data_mgr_flat.open_db()
        self.data_mgr_flat.send_data(params)
        self.data_mgr_flat.close_db()

    def _send2csv(self, params):
        # data_entry = ()
        # push data into FIFO
        # self.data_entries_csv.append(params)
        self.data_mgr_csv.open_db()
        self.data_mgr_csv.send_data(params)
        self.data_mgr_csv.close_db()
