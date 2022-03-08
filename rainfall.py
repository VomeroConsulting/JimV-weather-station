from gpiozero import Button
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

rain_sensor = Button(6)
BUCKET_SIZE = 0.2794
rain_count = 0


def bucket_tipped():
    global rain_count
    rain_count += 1
    logging.debug("Bucket tipped = {}".format(rain_count))


def get_and_reset_rainfall():
    global rain_count
    result = rain_count * BUCKET_SIZE
    rain_count = 0
    return result


rain_sensor.when_pressed = bucket_tipped

if __name__ == "__main__":
    import time

    rain_count = 0
    loop = 0

    while loop < 10:
        time.sleep(10)

        rainfall = get_and_reset_rainfall()
        print("Rainfall over 10 sec = {:.03f} mm".format(rainfall))
        loop += 1

    exit(0)
