from .opensky import AIRCRAFT_VECTOR_FIELDS

FIELS_OF_INTEREST = (
    "icao",
    "callsign",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "geo_altitude",
)


def clean_vector(raw_vector):
    clean = raw_vector[:]

    clean = dict(zip(AIRCRAFT_VECTOR_FIELDS, raw_vector))

    if None in (clean["longitude"], clean["latitude"]):
        # this is an invalid vector, ignore it
        return None

    return tuple([clean[key] for key in FIELS_OF_INTEREST])
