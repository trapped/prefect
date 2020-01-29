from sqlalchemy import Column, DateTime, String, Integer, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class AircraftVector(Base):
    # PK: a physical AC at a specific time
    __tablename__ = "aircraft_vector"

    icao = Column(String, primary_key=True)
    callsign = Column(String)
    origin_country = Column(String)
    time_position = Column(Integer)
    last_contact = Column(Integer, primary_key=True)
    longitude = Column(Float)
    latitude = Column(Float)
    baro_altitude = Column(String)
    on_ground = Column(Boolean)
    velocity = Column(Float)
    true_track = Column(Float)
    vertical_rate = Column(Float)
    geo_altitude = Column(Float)
    squawk = Column(String)
    spi = Column(Boolean)
    position_source = Column(Integer)


class Database:
    def __init__(self):
        self.engine = create_engine("sqlite:///aircraft-db.sqlite")
        session_maker = sessionmaker()
        session_maker.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self.session = session_maker()

    def add_aircraft_vectors(self, vectors):
        for entry in vectors:
            vec = AircraftVector(
                **dict(zip(AircraftVector.__table__.columns.keys(), entry[:]))
            )
            # insert or update vector
            self.session.merge(vec)

        # cannot bulk insert or update vectors
        # self.session.bulk_save_objects(objects)
        self.session.commit()
