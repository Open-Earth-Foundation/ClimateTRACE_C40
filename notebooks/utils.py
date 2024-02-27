import re
import pandas as pd
import tarfile
import os
import requests
from shapely.geometry import Point, shape
from shapely.wkt import loads
from sqlalchemy import text


def climatetrace_file_names():
    CLIMATETRACE_TAR_FILE = os.path.abspath("../data/raw/climateTRACE.tar.gz")
    with tarfile.open(CLIMATETRACE_TAR_FILE, "r:gz") as tar:
        return tar.getnames()


def load_climatetrace_file(path):
    """load climatetrace file as pandas dataframe"""
    CLIMATETRACE_TAR_FILE = os.path.abspath("../data/raw/climateTRACE.tar.gz")

    with tarfile.open(CLIMATETRACE_TAR_FILE, "r:gz") as tar:
        tar.extract(path)
        return pd.read_csv(path)


def get_c40_data():
    fl_C40 = os.path.abspath("../data/raw/C40_GPC_Database.xlsx")
    fl_cities = os.path.abspath("../data/processed/C40_cities.csv")

    sheet_name = "GHG Dashboard Data - Inventory"

    df_raw = pd.merge(
        pd.read_excel(fl_C40, sheet_name=sheet_name),  # raw C40 data
        pd.read_csv(fl_cities).dropna(subset=["locode"]),  # C40 city locodes
        on=["City", "Country"],
    )

    columns = [
        "City",
        "locode",
        "Year_calendar",
        "I.1.1",
        "I.1.2",
        "I.1.3",
        "I.2.1",
        "I.2.2",
        "I.2.3",
        "I.3.1",
        "I.3.2",
        "I.3.3",
        "I.4.1",
        "I.4.2",
        "I.4.3",
        "I.4.4",
        "I.5.1",
        "I.5.2",
        "I.5.3",
        "I.6.1",
        "I.6.2",
        "I.6.3",
        "I.7.1",
        "I.8.1",
        "II.1.1",
        "II.1.2",
        "II.1.3",
        "II.2.1",
        "II.2.2",
        "II.2.3",
        "II.3.1",
        "II.3.2",
        "II.3.3",
        "II.4.1",
        "II.4.2",
        "II.4.3",
        "II.5.1",
        "II.5.2",
        "II.5.3",
        "III.1.1",
        "III.1.2",
        "III.1.3",
        "III.2.1",
        "III.2.2",
        "III.2.3",
        "III.3.1",
        "III.3.2",
        "III.3.3",
        "III.4.1",
        "III.4.2",
        "III.4.3",
        "IV.1",
        "IV.2",
        "V.1",
        "V.2",
        "V.3",
        "VI.1",
    ]

    return df_raw[columns].rename(columns={"City": "city", "Year_calendar": "year"})


def filter_out_notation_keys(df, cols):
    if not isinstance(cols, list):
        cols = list(cols)

    notation_keys = {
        "IE": "Included Elsewhere",
        "NE": "Not Estimated",
        "NO": "Not Occurring",
        "C": "Confidential",
    }
    return df[~df[cols].apply(lambda row: any(row.isin(notation_keys.keys())), axis=1)]


def point_to_lat_lon(point):
    """extract lat lon from geoJSON point

    Parameters
    ----------
    point: str
        WKT representation

    Returns
    -------
    dic: dict
        dictionary with st_astext, lat, and lon
    """
    pattern = r"POINT\((?P<lon>[-\d.]+)\s+(?P<lat>[-\d.]+)\)"
    match = re.search(pattern, point)
    if match:
        lon = float(match.group("lon"))
        lat = float(match.group("lat"))
        return {"st_astext": point, "lon": lon, "lat": lat}
    else:
        print(f"ERROR: {point} does not conform to regular expresssion")


def lat_lon_inside_wkt(lat, lon, wkt):
    """test if lat lon is inside a WKT geometry

    Parameters
    ----------
    lat: float
        latitude value
    lon: float
        longitude value
    wkt: str
        geometry in well-known-text format

    Returns
    -------
    is_inside: bool
        boolean value indicating whether lat, lon is inside the WKT
    """
    point = Point(lon, lat)
    geometry = loads(wkt)
    return point.within(geometry)


def point_inside_wkt(point, wkt):
    """test if Point is inside a WKT geometry

    Parameters
    ----------
    point: str
        geojson point
    wkt: str
        geometry in well-known-text format

    Returns
    -------
    is_inside: bool
        boolean value indicating whether Point is inside the WKT
    """
    dic = point_to_lat_lon(point)
    lat, lon = dic["lat"], dic["lon"]
    return lat_lon_inside_wkt(lat, lon, wkt)


def lat_lon_to_locode_api(lat, lon):
    """converts a lat lon to a locode using cityboundary API

    Parameters
    ----------
    lat: float
        latitude value
    lon: float
        longitude value

    Returns
    -------
    locode: str
        the locode value
    """
    url = f"https://ccglobal.openearth.dev/api/v0/cityboundary/locode/{lat}/{lon}"
    response = requests.get(url)
    if response.status_code == 200:
        locodes = response.json()["locodes"]
        return locodes[0] if locodes else None
    else:
        return None


def lat_lon_to_locode(session, lat, lon):
    """converts a lat lon to a locode

    Parameters
    ----------
    lat: float
        latitude value
    lon: float
        longitude value
    session:
        sqlalchemy session

    Returns
    -------
    locode: str
        the locode value
    """
    query = text(
        """SELECT locode, geometry
                    FROM osm
                    WHERE :lat <= bbox_north
                    AND :lat >= bbox_south
                    AND :lon <= bbox_east
                    AND :lon >= bbox_west"""
    )
    result = session.execute(query, {"lat": lat, "lon": lon}).fetchall()
    for osm in result:
        if lat_lon_inside_wkt(lat, lon, osm["geometry"]):
            return osm["locode"]
    return None


def point_to_locode(session, point):
    """converts a Point to a locode

    Parameters
    ----------
    point: str
        geojson point
    session:
        sqlalchemy session

    Returns
    -------
    locode: str
        the locode value
    """
    coord_dic = point_to_lat_lon(point)
    lat, lon = coord_dic["lat"], coord_dic["lon"]
    return lat_lon_to_locode(session, lat, lon)
