from gpiozero import Button
import math
import time
import statistics


CM_IN_A_KM = 100000.0
SEC_IN_HOUR = 3600
MPH_CONVERSION = 1.609344
LIST_LENGHT = 3

wind_count = 0
NumberToAverage = 3
store_speeds = [0.0] * NumberToAverage

# Weather vain specific vaLues
radius_cm = 9.0
circumference_cm = 2 * math.pi * radius_cm
anemometer_factor = 1.18

# Programable variables
wind_count = 0  # Global var Counts rotations of anemometer
wind_measurement_time = 60  # In seconds, Report speed every 5 seconds


def spin():
    global wind_count
    wind_count = wind_count + 1
    # print("spin" + str(wind_count))


def calculate_speed(time_sec):
    global wind_count
    global temp
    rotations = wind_count / 2.0

    # Calculate distance
    dist_km = (circumference_cm * rotations) / CM_IN_A_KM
    speed_km_hr = (dist_km / time_sec) * SEC_IN_HOUR
    return speed_km_hr * anemometer_factor


wind_speed_sensor = Button(5)
wind_speed_sensor.when_activated = spin

print("CTRL C to exit")

while True:
    # store_speeds initialized to length of 3, keep popping off 1st
    # item as program appends new values to end of list
    store_speeds.pop(0)

    wind_count = 0
    time.sleep(wind_measurement_time)
    wind_current = calculate_speed(wind_measurement_time)
    store_speeds.append(wind_current)

    wind_gust = max(store_speeds)
    wind_average = statistics.mean(store_speeds)

    print(
        "Current: {:5.1f} km/hr, Average: {:5.1f} km/hr, Gust: {:5.1f}".format(
            wind_current, wind_average, wind_gust
        )
    )
