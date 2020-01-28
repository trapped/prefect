import math

# Semi-axes of WGS-84 geoidal reference
WGS84_a = 6378137.0  # Major semiaxis [m]
WGS84_b = 6356752.3  # Minor semiaxis [m]


class Position:
    lat = None
    long = None

    def __init__(self, lat, long):
        self.lat = lat
        self.long = long

    def validate(self):
        if self.lat < -90 or self.lat > 90:
            raise ValueError("Bad latitude given ({})".format(self.lat))

        if self.long < -180 or self.long > 180:
            raise ValueError("Bad longitude given ({})".format(self.long))


class Area:
    point1 = None
    point2 = None

    def __init__(self, point1, point2):
        self.point1 = point1
        self.point2 = point2

    def validate(self):
        for point in self.points:
            point.validate()

    @property
    def points(self):
        return (self.point1, self.point2)

    @property
    def lats(self):
        return (point.lat for point in self.points)

    @property
    def longs(self):
        return (point.long for point in self.points)

    @property
    def bounding_box(self):
        return (min(self.lats), max(self.lats), min(self.longs), max(self.longs))


# degrees to radians
def deg2rad(degrees):
    return math.pi * degrees / 180.0


# radians to degrees
def rad2deg(radians):
    return 180.0 * radians / math.pi


# Earth radius at a given latitude, according to the WGS-84 ellipsoid [m]
def wgs84_earth_radius(lat):
    An = WGS84_a * WGS84_a * math.cos(lat)
    Bn = WGS84_b * WGS84_b * math.sin(lat)
    Ad = WGS84_a * math.cos(lat)
    Bd = WGS84_b * math.sin(lat)
    return math.sqrt((An * An + Bn * Bn) / (Ad * Ad + Bd * Bd))


# Bounding box surrounding the point at given coordinates,
# assuming local approximation of Earth surface as a sphere
# of radius given by WGS84
def surrounding_area(position, half_side_in_km):
    lat = deg2rad(degrees=position.lat)
    lon = deg2rad(degrees=position.long)
    halfSide = 1000 * half_side_in_km

    # Radius of Earth at given latitude
    radius = wgs84_earth_radius(lat=lat)
    # Radius of the parallel at given latitude
    pradius = radius * math.cos(lat)

    latMin = lat - halfSide / radius
    latMax = lat + halfSide / radius
    lonMin = lon - halfSide / pradius
    lonMax = lon + halfSide / pradius

    return Area(
        point1=Position(rad2deg(latMin), rad2deg(lonMin)),
        point2=Position(rad2deg(latMax), rad2deg(lonMax)),
    )
