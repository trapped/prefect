from aircraftlib import (
    Position,
    surrounding_area,
    fetch_aircraft_vectors,
    Database,
    clean_vector,
)

# pull data, store to DB
# pull data periodically (hourly), append to DB
# sqlalchemy for ORM to sqlite local DB (build engine, session, and query objects)
# store raw vectors? or normallize in another way?
# pandas read to dataframe: df = pd.read_sql(query.statement, query.session.bind)


def main():
    # Fetch starting location based on airport (parameterize)
    # TODO: fetch from reference data
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)

    # Get the live AC vector data around this airport
    radius_km = 200
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)

    # Fetch the data
    print("fetching live vectors...")
    data = fetch_aircraft_vectors(area=area_surrounding_dulles)

    print("transform vectors...")
    transformed_vectors = []
    for raw_vector in data["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            transformed_vectors.append(vector)

    print("saving vectors...")
    db = Database()
    db.add_aircraft_vectors(transformed_vectors)


if __name__ == "__main__":
    main()
