import os
import mysql.connector as database
import time
from datetime import datetime


if os.environ.get("username") == None or os.environ.get("password") == None:
    username = "pi"
    password = "raspberry"
else:
    username = os.environ.get("username")
    password = os.environ.get("password")

db_name = "weather_data"

connection = database.connect(
    user=username, password=password, host="localhost", database=db_name
)

cursor = connection.cursor()


def add_data(
    w_speed_ave,
    w_speed_max,
    wind_dir,
    humidity,
    pressure,
    temperature,
    thermal_temp,
    rain,
):
    try:
        # statement = "INSERT INTO sensors (time,ws_ave) VALUES (%s, %s)"
        # data = (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), w_speed_ave)
        statement = "INSERT INTO sensors (ws_ave, ws_max, w_dir, humid, press, temp, therm, rain) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        data = (
            w_speed_ave,
            w_speed_max,
            wind_dir,
            humidity,
            pressure,
            temperature,
            thermal_temp,
            rain,
        )
        print(data)
        print(statement)
        cursor.execute(statement, data)
        connection.commit()
        print("Successfully added entry to database")
    except database.Error as e:
        print(f"Error adding entry to database: {e}")
        exit(1)


def get_data():
    try:
        statement = "SELECT * FROM sensors ORDER BY id DESC LIMIT 5"
        cursor.execute(statement)
        print("  id:time:ws_ave:ws_max:w_dir:humid:press:temp:therm:rain")
        for (
            id,
            time,
            ws_ave,
            ws_max,
            w_dir,
            humid,
            press,
            temp,
            therm,
            rain,
        ) in cursor:
            # ws_ave, ws_max, w_dir, humid,
            # pressure, temperature, thermal_temp, rain = x
            print(
                f"{id:4}:{time}:{ws_ave}:{ws_max}:{w_dir}:{humid}:{press}:{temp}:{therm}:{rain}"
            )
            # print(f"Successfully retrieved {first_name}, {last_name}")
    except database.Error as e:
        print(f"Error retrieving entry from database: {e}")


add_data(1.1, 2.2, "N", 3.3, 4.4, 5.5, 6.6, 7.7)
result = get_data()

connection.close()
exit(0)
