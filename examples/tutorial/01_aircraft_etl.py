from aircraftlib import Position, surrounding_area, fetch_aircraft, Database

# pull data, store to DB
# pull data periodically (hourly), append to DB
# sqlalchemy for ORM to sqlite local DB (build engine, session, and query objects)
# store raw vectors? or normallize in another way?
# pandas read to dataframe: df = pd.read_sql(query.statement, query.session.bind)


def main():
    # Define a position to start from
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)
    radius_km = 200
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)

    # Fetch the data
    print("fetching...")
    data = fetch_aircraft(area=area_surrounding_dulles)

    print("saving...")
    db = Database()
    db.add_aircraft_vectors(data["states"])


if __name__ == "__main__":
    main()
