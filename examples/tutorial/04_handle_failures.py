from datetime import timedelta

from aircraftlib import (
    Position,
    surrounding_area,
    fetch_live_aircraft_data,
    Database,
    clean_vector,
    add_airline_info,
    fetch_reference_data,
)

import prefect


@prefect.task(max_retries=3, retry_delay=timedelta(seconds=1))
def extract_reference_data():
    print("fetching reference data...")
    return fetch_reference_data()


@prefect.task(max_retries=3, retry_delay=timedelta(seconds=1))
def extract_live_data(airport, radius, ref_data):
    # Get the live AC vector data around the given airport (or none)
    area = None
    if airport:
        airport_data = ref_data.airports[airport]
        airport_position = Position(
            lat=float(airport_data["latitude"]), long=float(airport_data["longitude"])
        )
        area = surrounding_area(airport_position, radius)

    print("fetching live aircraft data...")
    raw_aircraft_data = fetch_live_aircraft_data(area=area, simulate_failures=2)

    return raw_aircraft_data


@prefect.task
def transform(raw_aircraft_data, ref_data):
    print("cleaning & transform aircraft data...")

    live_aircraft_data = []
    for raw_vector in raw_aircraft_data["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            add_airline_info(vector, ref_data.airlines)
            live_aircraft_data.append(vector)

    return live_aircraft_data


@prefect.task
def load_reference_data(ref_data):
    print("saving reference data...")
    db = Database()
    db.update_reference_data(ref_data)


@prefect.task
def load_live_data(live_aircraft_data):
    print("saving live aircraft data...")
    db = Database()
    db.add_live_aircraft_data(live_aircraft_data)


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
