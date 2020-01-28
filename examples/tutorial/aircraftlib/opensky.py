#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import calendar
import requests

from .position import Area


def fetch_aircraft(area=None):
    options = {"time": 0}

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

    response = requests.get(
        "https://opensky-network.org/api/states/all",
        auth=(),
        params=options,
        timeout=15.00,
    )
    response.raise_for_status()
    return response.json()
