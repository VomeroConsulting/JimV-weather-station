"""Weather Station code"""
import math
import time
import statistics
import logging

from datetime import datetime
from gpiozero import Button

# Sensor modules
import bme280_sensor
import ds18b20_therm

# local modules
# from ws_database import MariaDatabase
# from db_interface import MariaDatabase
import wind_direction
from data_mgr import DataMgr

"""
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    handlers=[logging.FileHandler("debug.log"), logging.StreamHandler()],
)
"""

# root logger to STDERR
# logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

# Create file logger to captuer errors
file_log = logging.FileHandler("weather_station.log", "w")
file_log.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
file_log.setFormatter(formatter)
logging.getLogger("").addHandler(file_log)


# Create tuples to identify measurement variables and optional DB columns
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

"""User must create a list of variable names in same order as column names.
Carefull, Code uses an eval() statement on these names to populate DB columns."""
vars2params = [
    "current_time",
    "wind_speed_average",
    "wind_speed_gust",
    "wind_direction_value",
    "humidity",
    "pressure",
    "temperature",
    "ground_temperature",
    "rainfall",
]

# Global variables
wind_count = 0  # Global var Counts rotations of anemometer
rain_count = 0  # Global to count number of times rain bucket tips

"""Wind speed and direction measurements are sampled over a period of time.
ToDo: Add statement why time will slip with time/interval values"""
WIND_MEASUREMENT_TIME = 7  # In seconds, Report speed every 7 seconds
WIND_MEASUREMENT_INTERVAL = 1  # In minutes, Measurements recorded every 5 minutes

# Constants
CM_IN_A_KM = 100000.0
SEC_IN_HOUR = 3600
MPH_CONVERSION = 1.609344
ANEMOMETER_FACTOR = 1.18
BUCKET_SIZE = 0.2794
WIND_SPEED_SENSOR_BUTTON = 5
RAIN_SENSOR_BUTTON = 6

# Weather vain specific vaLues
RADIUS_CM = 9.0  # radius of anemometer
CIRCUMFERENCE_CM = 2 * math.pi * RADIUS_CM

# Interrupt process for Spin connected to GPIO(5)
def spin():
    global wind_count
    wind_count += 1


wind_speed_sensor = Button(WIND_SPEED_SENSOR_BUTTON)
wind_speed_sensor.when_activated = spin


# Interrupt process for Rain Bucket being tipped
def bucket_tipped():
    global rain_count
    rain_count += 1


rain_sensor = Button(RAIN_SENSOR_BUTTON)
rain_sensor.when_pressed = bucket_tipped


def get_and_reset_rainfall():
    global rain_count
    result = rain_count * BUCKET_SIZE
    rain_count = 0
    return result


# WindSpeedDirThread class used for measurements requiring integration over time.
class WindSpeedDirThread:
    """
    thread_name: Arbitrary name
    measurement_time: is seconds, period of time to make 1 measurement, recomdnd % 60
    measurement_count: is number of measurements to aquire
    """

    def __init__(self, measurement_time, measurement_count):
        # super().__init__()
        # Calling the Thread class's init function
        # Thread.__init__(self)
        # self.thread_name = thread_name
        self.measurement_time = measurement_time
        self.measurement_count = measurement_count
        self.wind_direction_data = []
        self.wind_speed_data = []

    def calculate_speed(self):
        rotations = wind_count / 2.0

        # Calculate distance
        dist_km = (CIRCUMFERENCE_CM * rotations) / CM_IN_A_KM
        speed_km_hr = (dist_km / self.measurement_time) * SEC_IN_HOUR
        return speed_km_hr * ANEMOMETER_FACTOR

    def calculate_dir(self):
        direction = wind_direction.WindDirection()
        result = direction.get_direction()

        # try at least twice for a good direction result
        if result == None:
            # None indicate a strange corner case changing between 2 values
            logging.debug("*** Direction Re-Measure ***")
            time.sleep(0.01)
            return direction.get_direction()
        else:
            return result

    def run(self):
        logging.info("Starting Wind/Direction Measurement")
        # print("Starting {} Thread".format(self.thread_name))
        global wind_count
        self.wind_direction_data = []
        self.wind_speed_data = []
        loop_count = self.measurement_count

        while loop_count > 0:
            logging.info("Loop {} = {:.03f}".format(loop_count, time.time()))
            wind_count = 0
            time.sleep(self.measurement_time)
            temp = self.calculate_speed()
            self.wind_speed_data.append(temp)
            logging.debug("Wind Speed = {:.01f}".format(temp))
            # self.wind_speed.append(self.calculate_speed())
            value = self.calculate_dir()
            # value = WindDirection.get_direction()
            self.wind_direction_data.append(value)
            logging.debug("Wind Dir = %s", value)
            # logging.debug("Wind Dir = {}".format(value))
            # store_speeds.append(speed_km_hr)
            loop_count -= 1
        return

    def get_wind_speed_average(self):
        return statistics.mean(self.wind_speed_data)

    def get_wind_speed_gust(self):

        return max(self.wind_speed_data)

    def get_wind_dir_mode(self):
        return statistics.mode(self.wind_direction_data)


"""kwargs is passed to db_mgr and are used to configure how data is stored"""


def main(**kwargs):

    # Initialize all measurement objects
    speed_and_dir = WindSpeedDirThread(
        WIND_MEASUREMENT_TIME,
        WIND_MEASUREMENT_INTERVAL * (int(60 / WIND_MEASUREMENT_TIME)),
    )

    # Temperature, humidity, and pressure sensor
    # and Ground thermal sensor
    bme = bme280_sensor.temperature_sensor()
    therm = ds18b20_therm.DS18B20()

    # db_mgr opens Maria DB, optionally opens files to store CVS or jswon data.
    # Must exit from main() if data cannot be saved.
    try:
        db_mgr = DataMgr(column_names, **kwargs)
    except Exception as e:
        logging.error("Error Cannot open data storage: %s", e)
        raise

    # Just to be neat, Set start time to occur when second changes
    start_time = int(time.time()) + 1
    logging.debug("Start Time = {:.03f}".format(start_time))

    # Infinite loop to measure sensor values CTRL-C required to exit.
    while True:
        logging.debug(
            "Start Time {:.03f} ; Current Time {:.03f}".format(start_time, time.time())
        )

        while start_time > time.time():
            time.sleep(0.01)

        # Run measurements for wind speed and direction
        # This measurement runs for WIND_MEASUREMENT_INTERVAL minutes
        speed_and_dir.run()

        # Collect data from other sensors
        logging.info("Time of Measurement = {:.03f}".format(time.time()))
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        humidity, pressure, temperature = bme.read_all_bme820()
        humidity = round(humidity, 1)
        pressure = round(pressure, 1)
        temperature = round(temperature, 1)
        logging.info("Humidity = {:.01f}".format(humidity))
        logging.info("Pressure = {:.01f}".format(pressure))
        logging.info("Temperature = {:.01f}".format(temperature))

        ground_temperature = round(therm.read_temp(), 1)
        logging.info("Ground Temperature = {:.01f}".format(ground_temperature))

        rainfall = round(get_and_reset_rainfall(), 3)
        logging.info("Rainfall = {:.03f}".format(rainfall))

        wind_speed_average = speed_and_dir.get_wind_speed_average()
        wind_speed_average = round(wind_speed_average, 1)
        wind_speed_gust = speed_and_dir.get_wind_speed_gust()
        wind_speed_gust = round(wind_speed_gust, 1)
        wind_direction_value = speed_and_dir.get_wind_dir_mode()

        logging.info("Wind Speed = %.01f", wind_speed_average)
        logging.info("Wind Gust = %.01f", wind_speed_gust)
        logging.info("Wind Direction = %3s", wind_direction_value)

        """
        # User can create a list of values based on measurement values
        # or used the eval code below
        params = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            wind_speed_average,
            wind_speed_gust,
            wind_direction_value,
            humidity,
            pressure,
            temperature,
            ground_temperature,
            rainfall,
        ]
        """

        # Loop through user supplyed variable names that contain the weather
        # station data and put into a list. Must evaluate "name" to get the data.
        params = []
        for var in vars2params:
            params.append(eval(var))

        # update_entries requires data formated as tuple
        db_mgr.update_entries(tuple(params))

        # Calculate next start time
        start_time += WIND_MEASUREMENT_INTERVAL * 60
        logging.info("Next Start Time = {:.03f}".format(start_time))


if __name__ == "__main__":
    from datetime import datetime

    try:
        # json_file_name = None
        json_file_name = "json_backend_private.load"

        # configure file path
        file_path = "/mnt/NAS/Weather_data/"
        file_date = (datetime.today()).strftime("%y%m%d")
        flat_config_args = file_path + "flat_data." + file_date + ".txt"
        csv_config_args = file_path + "csv_data." + file_date + ".txt"

        # main(db_config=json_file_name)
        # main(db_config=json_file_name, csv_config="/tmp/test_csv.txt")
        # main(db_config=json_file_name, flat_config="/tmp/test_flat.txt")
        main(
            db_config=json_file_name,
            csv_config=csv_config_args,
            flat_config=flat_config_args,
        )
        # main(csv_config="/tmp/test_csv.csv")
    except Exception as e:
        logging.exception("Exception in main(): ")
        logging.warning("Error Exception in main(): %s", e)
