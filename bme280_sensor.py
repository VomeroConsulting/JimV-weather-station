# #! /usr/bin/python3.9

from time import sleep

import bme280
import smbus2


port = 1
address = 0x77  # Adafruit BME280 address. Other BME280s may be different
bus = smbus2.SMBus(port)

# bme280_load_calibration_params(bus, address)


def read_all_bme820():
    bme280_data = bme280.sample(bus, address)
    return bme280_data.humidity, bme280_data.pressure, bme280_data.temperature


humidity, pressure, temperature = read_all_bme820()

print(
    "Humidy = ",
    humidity,
    " Pressure = ",
    pressure,
    "Temp = ",
    temperature,
)

exit(0)
