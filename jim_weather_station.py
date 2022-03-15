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
from ws_database import maria_database
import wind_direction

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")
# logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

# Create tuples to identify measurement variables and optional DB columns
measure_var = (
    "date_and_time",
    "wind_speed_average",
    "wind_speed_gust",
    "wind_direction_value",
    "humidity",
    "pressure",
    "temperature",
    "ground_temperature",
    "rain",
)

db_columns = (
    "time",
    "ws_ave",
    "ws_max",
    "w_dir",
    "humid",
    "press",
    "temp",
    "therm",
    "rain",
)

# Global variables
wind_count = 0  # Global var Counts rotations of anemometer
wind_measurement_time = 5  # In seconds, Report speed every 5 seconds
wind_measurement_interval = 1  # In minutes, Measurements recorded every 5 minutes
rain_count = 0  # Global to count number of times rain bucket tips

# Constants
CM_IN_A_KM = 100000.0
SEC_IN_HOUR = 3600
MPH_CONVERSION = 1.609344
ANEMOMETER_FACTOR = 1.18
BUCKET_SIZE = 0.2794
WIND_SPEED_SENSOR_BUTTON = 5
RAIN_SENSOR_BUTTON = 6

# Weather vain specific vaLues
radius_cm = 9.0  # radius of anemometer
circumference_cm = 2 * math.pi * radius_cm

# Interrupt process for Spin connected to GPIO(5)
def spin():
    global wind_count
    wind_count += 1


wind_speed_sensor = Button(WIND_SPEED_SENSOR_BUTTON)
wind_speed_sensor.when_activated = spin


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
    # class WindSpeedDirThread(Thread):
    # class WindSpeedDirThread(threading.Thread):
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
        # rotations = wind_count / 2.0
        rotations = wind_count

        # Calculate distance
        dist_km = (circumference_cm * rotations) / CM_IN_A_KM
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


def main():
    # Calculate the number of wind measurements that are made over the measurement time
    # and pass to class initialization
    # TBD Should number of number of measurements be such that
    # time will not continue to slip?
    speed_and_dir = WindSpeedDirThread(
        wind_measurement_time,
        wind_measurement_interval * (int(60 / wind_measurement_time)),
    )

    # Temperature, humidity, and pressure sensor
    # and Ground thermal sensor
    bme = bme280_sensor.temperature_sensor()
    therm = ds18b20_therm.DS18B20()

    # Set start time to occur when second changes
    start_time = int(time.time()) + 1
    logging.debug("Start Time = {:.03f}".format(start_time))

    # db = maria_database()

    try:
        db = maria_database()
        db.open_db()
    except Exception as e:
        logging.warning("Error opening MariaDB(): %s", e)

    while True:
        logging.debug(
            "Start Time {:.03f} ; Current Time {:.03f}".format(start_time, time.time())
        )
        # Assures that measurements are intergrated over min time period
        while start_time > time.time():
            time.sleep(0.01)
            # logging.debug("Waiting Start time")

        # Run measurements for wind speed and direction
        speed_and_dir.run()

        # Wind speed and direction are measured over time and are now complete.
        # Capture time stamp and all measurements.
        # params = [time.strftime("%Y-%m-%d %H:%M:%S")]

        # Collect data from other sensors
        logging.info("Time of Measurement = {:.03f}".format(time.time()))
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

        # Write to Weather Station DB
        db.write_db(params)

        # Calculate next start time
        start_time += wind_measurement_interval * 60

        logging.info("Next Start Time = {:.03f}".format(start_time))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("Exception in main(): ")
        logging.warning("Error Exception in main(): %s", e)
