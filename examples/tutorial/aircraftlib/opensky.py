#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import calendar
import requests

from .position import Area


def _api_request_json(req, options=None):
    response = requests.get(
        "https://opensky-network.org/api/{}".format(req),
        auth=(),
        params=options or {},
        timeout=5.00,
    )
    response.raise_for_status()
    return response.json()


def fetch_aircraft(area=None):
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
