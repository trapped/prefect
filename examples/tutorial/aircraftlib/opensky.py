#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import calendar
import requests

from .position import Area


# OpenSky API states/all reference
# Source: https://opensky-network.org/apidoc/rest.html#all-state-vectors
# 0  icao24             string      Unique ICAO 24-bit address of the transponder in hex string representation.
# 1  callsign           string      Callsign of the vehicle (8 chars). Can be null if no callsign has been received.
# 2  origin_country     string      Country name inferred from the ICAO 24-bit address.
# 3  time_position      int         Unix timestamp (seconds) for the last position update. Can be null if no position report was received by OpenSky within the past 15s.
# 4  last_contact       int         Unix timestamp (seconds) for the last update in general. This field is updated for any new, valid message received from the transponder.
# 5  longitude          float       WGS-84 longitude in decimal degrees. Can be null.
# 6  latitude           float       WGS-84 latitude in decimal degrees. Can be null.
# 7  baro_altitude      float       Barometric altitude in meters. Can be null.
# 8  on_ground          boolean     Boolean value which indicates if the position was retrieved from a surface position report.
# 9  velocity           float       Velocity over ground in m/s. Can be null.
# 10 true_track         float       True track in decimal degrees clockwise from north (north=0°). Can be null.
# 11 vertical_rate      float       Vertical rate in m/s. A positive value indicates that the airplane is climbing, a negative value indicates that it descends. Can be null.
# 12 sensors            int[]       IDs of the receivers which contributed to this state vector. Is null if no filtering for sensor was used in the request.
# 13 geo_altitude       float       Geometric altitude in meters. Can be null.
# 14 squawk             string      The transponder code aka Squawk. Can be null.
# 15 spi                boolean     Whether flight status indicates special purpose indicator.
# 16 position_source    int         Origin of this state’s position: 0 = ADS-B, 1 = ASTERIX, 2 = MLAT

AIRCRAFT_VECTOR_FIELDS = (
    "icao",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
)


def _api_request_json(req, options=None):
    response = requests.get(
        "https://opensky-network.org/api/{}".format(req),
        auth=(),
        params=options or {},
        timeout=5.00,
    )
    response.raise_for_status()
    return response.json()


def fetch_aircraft_vectors(area=None):
    options = {}
    if area != None:
        if isinstance(area, Area):
            area.validate()
            area_fields = area.bounding_box

            options["lamin"] = area_fields[0]
            options["lamax"] = area_fields[1]
            options["lomin"] = area_fields[2]
            options["lomax"] = area_fields[3]
        else:
            raise ValueError("Bad area given")

    return _api_request_json("states/all", options=options)
