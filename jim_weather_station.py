from gpiozero import Button
import math
import time
import statistics
import bme280_sensor
import wind_direction_byo
import ds18b20_therm


wind_count = 0  # Counts rotations of anemometer
wind_interval = 5  # Report speed every 5 seconds
interval = 5  # Measurements recorded every 5 minutes

CM_IN_A_KM = 100000.0
SEC_IN_HOUR = 3600
MPH_CONVERSION = 1.609344
ANEMOMETER_FACTOR = 1.18
BUCKET_SIZE = 0.2794
LIST_LENGHT = 3

store_speeds = []
Number_to_average = 0

# Weather vain specific vaLues
radius_cm = 9.0  # radius of anemometer
circumference_cm = 2 * math.pi * radius_cm

# Programable variables


def spin():
    global wind_count
    wind_count = wind_count + 1
    # print("spin" + str(wind_count))


def calculate_speed(time_sec):
    global wind_count
    global temp
    # rotations = wind_count / 2.0
    rotations = wind_count

    # Calculate distance
    dist_km = (circumference_cm * rotations) / CM_IN_A_KM
    speed_km_hr = (dist_km / time_sec) * SEC_IN_HOUR
    # speed_km_hr = speed_km_sec * SEC_IN_HOUR
    # speed = speed_km_hr * anemometer_factor

    return speed_km_hr * anemometer_factor


wind_speed_sensor = Button(5)
wind_speed_sensor.when_activated = spin

while True:
    start_time = time.time()
    # while time.time() - start_time <= interval:

    # average wind speed over LIST_LENGET samples
    if Number_to_average < LIST_LENGHT:
        Number_to_average = len(store_speeds)
    else:
        store_speeds.pop(0)

    wind_count = 0
    wind_direction = []

    time.sleep(wind_interval)
    speed_km_hr = calculate_speed(wind_interval)
    store_speeds.append(speed_km_hr)

    wind_gust = max(store_speeds)
    wind_speed = statistics.mean(store_speeds)

    # speed_mi_hr = speed_km_hr / MPH_CONVERSION
    # 160,934.5 cm in mile, 3600 sec in hour
    # speed_mi_hr = (speed_cm_sec / 160934.4) * 3600
    print("{:5.1f} km/hr, {:5.1f} km/hr".format(speed_km_hr, wind_gust))
    # print("{:5.1f} km/hr : {5.1f} km/hr".format(wind_speed, wind_gust))
    print("{:5.1f} km/hr".format(wind_speed))
    print("{:5.1f} km/hr".format(wind_gust))
    # print("{:5.1f}, mi/hr".format(speed_mi_hr))
