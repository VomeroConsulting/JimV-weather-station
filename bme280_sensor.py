from time import sleep

import bme280
import smbus2


class temperature_sensor:
    def __init__(self):
        self.port = 1
        self.address = 0x77  # Adafruit BME280 address. Other BME280s may be different
        self.bus = smbus2.SMBus(self.port)

    def read_all_bme820(self):
        bme280_data = bme280.sample(self.bus, self.address)
        return bme280_data.humidity, bme280_data.pressure, bme280_data.temperature

    # bme280_load_calibration_params(bus, address)


if __name__ == "__main__":
    bme = temperature_sensor()
    humidity, pressure, temperature = bme.read_all_bme820()

    print(
        "Humidy = ",
        humidity,
        " Pressure = ",
        pressure,
        "Temp = ",
        temperature,
    )

    exit(0)
