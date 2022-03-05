# *** Weather Station 2021 ***

# Code adapted from the following in some instances:
# @ https://projects.raspberrypi.org/en/projects/build-your-own-weather-station/7

from gpiozero import MCP3008
import logging


class WindDirection:
    def __init__(self):
        self.vane = MCP3008(channel=0)
        self.volts = {
            0.4: "N",
            1.4: "NNE",
            1.2: "NE",
            2.8: "ENE",
            2.7: "E",
            2.9: "ESE",
            2.2: "SE",
            2.5: "SSE",
            1.8: "S",
            2.0: "SSW",
            0.7: "SW",
            0.8: "WSW",
            0.1: "W",
            0.3: "WNW",
            0.2: "NW",
            0.6: "NNW",
        }

    """ Wind Direction uses a set of reed switches to create a voltage divider.
    A magnet in the vane can close 1 or 2 switchs thus providing 16 values.
    It is possible for a measurement to occur when voltages are changing thus
    producing a invalid result (None)"""

    def get_direction(self):
        reading_raw = self.vane.value * 3.3
        reading = round(reading_raw, 1)
        if reading in self.volts:
            logging.info("A2D = {}".format(reading))
            result = self.volts[reading]
            # print(result, reading, reading_raw)
            return result
        else:
            logging.info("A2D = {} - None".format(reading))
            # print("Vane output not in list:", reading, reading_raw)
            return None


if __name__ == "__main__":
    import time

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s"
    )

    d = WindDirection()
    loop = 0

    while count < 100:
        direction = d.get_direction()
        logging.info("Wind Dir = {}".format(direction))

        time.sleep(0.5)
        loop += 1

    exit(0)
