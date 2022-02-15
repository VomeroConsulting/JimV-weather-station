# *** Weather Station 2021 ***

# Code adapted from the following in some instances:
# @ https://projects.raspberrypi.org/en/projects/build-your-own-weather-station/7

from gpiozero import MCP3008


class wind_direction:
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

    def get_direction(self):
        reading_raw = self.vane.value * 3.3
        reading = round(reading_raw, 1)
        if reading in self.volts:
            result = self.volts[reading]
            print(result, reading, reading_raw)
            # return result
            # return result, reading
            return result, reading, reading_raw
        else:
            print("Vane output not in list:", reading, reading_raw)
            # return ("None", reading)
            return "None", reading, reading_raw
