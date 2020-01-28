from aircraftlib import Position, surrounding_area, fetch_aircraft


def main():
    # Define a position to start from
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)
    radius_km = 20
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)

    # Fetch the data
    data = fetch_aircraft(area=area_surrounding_dulles)

    # Do something with the data
    print(data)


if __name__ == "__main__":
    main()
