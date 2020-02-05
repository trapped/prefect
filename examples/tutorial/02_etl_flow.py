from aircraftlib import (
    Position,
    surrounding_area,
    fetch_live_aircraft_data,
    Database,
    clean_vector,
    add_airline_info,
    fetch_reference_data,
)

from prefect import Flow, task


@task
def extract_reference_data():
    print("fetching reference data...")
    return fetch_reference_data()


@task
def extract_live_data():
    # Get the live AC vector data around Dulles airport
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)
    radius_km = 200
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)

    print("fetching live aircraft data...")
    raw_aircraft_data = fetch_live_aircraft_data(area=area_surrounding_dulles)

    return raw_aircraft_data


@task
def transform(raw_aircraft_data, ref_data):
    print("cleaning & transform aircraft data...")

    live_aircraft_data = []
    for raw_vector in raw_aircraft_data["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            add_airline_info(vector, ref_data.airlines)
            live_aircraft_data.append(vector)

    return live_aircraft_data


@task
def load_reference_data(ref_data):
    print("saving reference data...")
    db = Database()
    db.update_reference_data(ref_data)


@task
def load_live_data(live_aircraft_data):
    print("saving live aircraft data...")
    db = Database()
    db.add_live_aircraft_data(live_aircraft_data)


def main():
    with Flow("etl") as flow:
        reference_data = extract_reference_data()
        live_data = extract_live_data()

        transformed_live_data = transform(live_data, reference_data)

        load_reference_data(reference_data)
        load_live_data(transformed_live_data)

    flow.run()


if __name__ == "__main__":
    main()
