from aircraftlib import (
    Position,
    surrounding_area,
    fetch_aircraft_vectors,
    Database,
    clean_vector,
    add_airline_info,
    fetch_airlines,
    fetch_routes,
    fetch_equipment,
    fetch_airports,
)

import prefect


@prefect.task
def extract_live_data():
    # Fetch starting location based on airport (parameterize)
    # TODO: fetch position from reference data
    dulles_airport_position = Position(lat=38.9519444444, long=-77.4480555556)

    # Get the live AC vector data around this airport
    radius_km = 200
    area_surrounding_dulles = surrounding_area(dulles_airport_position, radius_km)
    # area_surrounding_dulles = None

    print("fetching live aircraft vectors...")
    raw_ac_vectors = fetch_aircraft_vectors(
        area=area_surrounding_dulles
    )  # , offline=True)

    return raw_ac_vectors


@prefect.task
def extract_reference_data():

    print("fetching reference data...")
    airlines_ref_data = fetch_airlines()
    airports_ref_data = fetch_airports()
    route_ref_data = fetch_routes()
    equipment_ref_data = fetch_equipment()

    return (airlines_ref_data, airports_ref_data, route_ref_data, equipment_ref_data)


@prefect.task
def transform(raw_ac_vectors, reference_data):

    (
        airlines_ref_data,
        airports_ref_data,
        route_ref_data,
        equipment_ref_data,
    ) = reference_data

    print("cleaning & transform vectors...")
    transformed_vectors = []
    for raw_vector in raw_ac_vectors["states"]:
        vector = clean_vector(raw_vector)
        if vector:
            add_airline_info(vector, airlines_ref_data)
            transformed_vectors.append(vector)

    return transformed_vectors


@prefect.task
def load_reference_data(reference_data):
    (
        airlines_ref_data,
        airports_ref_data,
        route_ref_data,
        equipment_ref_data,
    ) = reference_data

    print("saving reference data...")
    db = Database()
    db.update_airlines(airlines_ref_data)
    db.update_airports(airports_ref_data)
    db.update_routes(route_ref_data)
    db.update_equipment(equipment_ref_data)


@prefect.task
def load_live_data(transformed_vectors,):
    print("saving vectors...")
    db = Database()
    db.add_aircraft_vectors(transformed_vectors)


def main():
    with prefect.Flow("etl") as flow:

        reference_data = extract_reference_data()
        live_data = extract_live_data()
        transformed_live_data = transform(live_data, reference_data)
        load_reference_data(reference_data)
        load_live_data(transformed_live_data)

    flow.run()


if __name__ == "__main__":
    main()
