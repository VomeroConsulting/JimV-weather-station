from wind_direction import wind_direction
import time
import signal


def handler(signum, frame):
    print("CTRL-C Pressed")
    f.write("\nEND\n")
    seperator = "\t"
    s = ""
    # for i in vane_readings:
    # s += "\t".join(str(i))
    s = seperator.join(vane_readings)
    print(s)
    f.write(s)
    f.write("\n")

    s = ""
    s = seperator.join([str(x) for x in vane_results])
    # for i in vane_results:
    # s += "\t".join(str(i))
    print(s)
    f.write(s)
    f.write("\n")

    print(vane_readings)
    print(vane_results)
    f.flush()
    f.close()
    exit(1)


signal.signal(signal.SIGINT, handler)

weather_vane = wind_direction()

vane_readings = []
vane_results = []
last_vane_reading = "First"
last_vane_result = "First"

f = open("datalog.txt", "a")

while True:
    t1 = time.time()
    # for _ in range(10000):
    # vane_readings.append(weather_vane.get_direction())
    # reading, results = weather_vane.get_direction()
    reading, results, reading_raw = weather_vane.get_direction()
    if reading not in vane_readings:
        vane_readings.append(reading)
        last_vane_reading = reading

    if results not in vane_results:
        vane_results.append(results)
        last_vane_result = results

    print(vane_readings)
    print(vane_results)
    print()

    t1 = round(t1, 2)

    write_str = "{}\t{}\t{}\t{}\n"
    f.write(write_str.format(str(t1), reading, str(results), str(reading_raw)))
    # f.write(str(t1), "\t", reading, "\t", str(results), "\t", str(reading_raw))
    # print("T1 = {0:.02f}".format(t1))

    time.sleep(0.2)

print(vane_readings)
f.close()
