from asyncio import sleep
from gpiozero import MCP3008
import time

adc = MCP3008(channel=0)

count = 0
values = []
volts = {
    0.4: 0.0,
    1.4: 22.5,
    1.2: 45.0,
    2.8: 67.5,
    2.7: 90.0,
    2.2: 135.0,
    2.5: 157.5,
    1.8: 180.0,
    2.0: 202.5,
    0.7: 225.0,
    0.8: 247.5,
    0.1: 270.0,
    0.3: 292.5,
    0.2: 315.0,
    0.6: 337.5,
}

while True:
    time.sleep(1)
    wind = round(adc.value * 3.3, 1)
    if not wind in volts:
        print("Unknown {:}".format(str(wind)))
        wind = round(adc.value * 3.3, 1)
        if not wind in volts:
            print("Double Unknown {:}".format(str(wind)))

print(adc.value)
