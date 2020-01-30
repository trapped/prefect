from aircraftlib import (
    Position,
    surrounding_area,
    fetch_aircraft_vectors,
    Database,
    clean_vector,
    add_airline_info,
    fetch_reference_data,
)

# pull data, store to DB
# pull data periodically (hourly), append to DB
# sqlalchemy for ORM to sqlite local DB (build engine, session, and query objects)
# store raw vectors? or normallize in another way?
# pandas read to dataframe: df = pd.read_sql(query.statement, query.session.bind)

# ETL:
#   - progress to parameters:
#       - Fetch within an area (position, radius, disable)
#       - Enable/Disable update reference data

# analysis:
#   - how many AC from each airline are in the sky right now?
#   - Frequency management via clustering
#   - Plot current flights (just dots)
#   - Plot current routes (great circle distance)
#   - Correlate current flights with known routes
#   - Plot pariclar AC path over time (requires historical data)


def main():
    # Get the live AC vector data around Dulles airport
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)
    radius_km = 200
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)

    print("fetching reference data...")
    ref_data = fetch_reference_data()

    print("fetching live aircraft vectors...")
    raw_aircraft_vectors = fetch_aircraft_vectors(area=area_surrounding_dulles)

    print("cleaning & transform vectors...")
    aircraft_vectors = []
    for raw_vector in raw_aircraft_vectors["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            add_airline_info(vector, ref_data.airlines)
            aircraft_vectors.append(vector)

    print("saving vectors...")
    db = Database()
    db.add_aircraft_vectors(aircraft_vectors)

    print("saving reference data...")
    db.update_reference_data(ref_data)

    print("complete!")


if __name__ == "__main__":
    main()
