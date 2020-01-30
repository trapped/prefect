from aircraftlib import (
    Position,
    surrounding_area,
    fetch_aircraft_vectors,
    Database,
    clean_vector,
    add_airline_info,
    fetch_reference_data,
)

import prefect


@prefect.task
def extract_reference_data():
    print("fetching reference data...")
    return fetch_reference_data()


@prefect.task
def extract_live_data(airport, radius, ref_data):
    # Get the live AC vector data around the given airport (or none)
    area = None
    if airport:
        airport_data = ref_data.airports[airport]
        airport_position = Position(
            lat=float(airport_data["latitude"]), long=float(airport_data["longitude"])
        )
        area = surrounding_area(airport_position, radius)

    print("fetching live aircraft vectors...")
    raw_aircraft_vectors = fetch_aircraft_vectors(area=area)

    return raw_aircraft_vectors


@prefect.task
def transform(raw_aircraft_vectors, ref_data):
    print("cleaning & transform vectors...")

    aircraft_vectors = []
    for raw_vector in raw_aircraft_vectors["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            add_airline_info(vector, ref_data.airlines)
            aircraft_vectors.append(vector)

    return aircraft_vectors


@prefect.task
def load_reference_data(ref_data):
    print("saving reference data...")
    db = Database()
    db.update_reference_data(ref_data)


@prefect.task
def load_live_data(aircraft_vectors):
    print("saving vectors...")
    db = Database()
    db.add_aircraft_vectors(aircraft_vectors)


def main():
    with prefect.Flow("etl") as flow:
        airport = prefect.Parameter("airport", default="IAD")
        radius = prefect.Parameter("radius", default=200)

        reference_data = extract_reference_data()
        live_data = extract_live_data(airport, radius, reference_data)

        transformed_live_data = transform(live_data, reference_data)

        load_reference_data(reference_data)
        load_live_data(transformed_live_data)

    flow.run(airport="DCA", radius=10)


if __name__ == "__main__":
    main()
