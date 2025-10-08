#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Traza rutas aéreas desde Excel usando un índice local de aeropuertos en CSV.
Requisitos: pandas, folium, openpyxl
"""

from pathlib import Path
import argparse
import math
import json
import time
import copy
from collections import Counter
from datetime import datetime
from difflib import SequenceMatcher
import unicodedata
import urllib.parse
import subprocess
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from branca.element import MacroElement, Template
import requests

def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def load_airport_index(csv_path: Path, prefix_region_map=None):
    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de aeropuertos: {csv_path}")
    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        raise RuntimeError(f"No se pudo leer {csv_path}: {exc}") from exc

    index = {}

    for _, row in df.iterrows():
        name = str(row.get("airport") or "").strip()
        iata_raw = str(row.get("iata") or "").strip().upper()
        iata = iata_raw or None
        icao_raw = str(row.get("icao") or "").strip().upper()
        if icao_raw in {"", "NAN", "NONE", "NULL"}:
            icao = None
        else:
            icao = icao_raw
        lat = to_float(row.get("latitude"))
        lon = to_float(row.get("longitude"))
        country = str(row.get("country_code") or row.get("country") or "").strip().upper() or None
        region = str(row.get("region_name") or "").strip()
        country_name = str(row.get("country") or row.get("country_name") or "").strip()
        region_letter = None
        if icao:
            region_letter = icao[0]
            if prefix_region_map:
                prefix_info = prefix_region_map.get(icao[:2])
                if prefix_info:
                    pref_region, pref_country = select_prefix_candidate(prefix_info, country_name, country, name)
                    if pref_region:
                        region = pref_region
                    if pref_country:
                        country_name = pref_country or country_name
        if (not region) and prefix_region_map and icao and len(icao) >= 2:
            prefix_info = prefix_region_map.get(icao[:2])
            if prefix_info:
                pref_region, pref_country = select_prefix_candidate(prefix_info, country_name, country, name)
                if pref_region:
                    region = pref_region or region
                if pref_country:
                    country_name = pref_country or country_name
        if not region and region_letter:
            region = ICAO_REGION_LABELS.get(region_letter, region_letter)
        rec = {
            "name": name,
            "iata": iata,
            "icao": icao,
            "latitude": lat,
            "longitude": lon,
            "region": region or None,
            "region_letter": region_letter,
            "country_code": country,
            "country_name": country_name or None,
        }
        for code in (iata, icao):
            if code and code not in index:
                index[code] = rec
    return index

EARTH_RADIUS_KM = 6371.0088
KM_TO_NM = 0.539957
SECTOR_CACHE_PATH = Path("./sector_lecbe.json")
SECTOR_SERVICE_URL = (
    "https://servais.enaire.es/insignia/rest/services/INSIGNIA_SRV/"
    "Aero_SRV_VIGOR_Sectores_V2_1/MapServer/2/query"
)
WORLD_GEOJSON_PATH = Path("./world_countries.geojson")
WORLD_GEOJSON_URL = "https://raw.githubusercontent.com/datasets/geo-boundaries-world-110m/master/countries.geojson"

ALTITUDE_THRESHOLD_FT = 2000.0
ALTITUDE_CATEGORY_LABELS = {
    "ascenso": "Ascensos (>2000 ft)",
    "descenso": "Descensos (>2000 ft)",
    "sobrevuelo": "Sobrevuelos (±2000 ft)",
}
FLOW_LAYER_STYLES = {
    "all": {"name": "Flujos principales", "color": "#5e35b1", "show": True},
    "ascenso": {"name": "Flujos ascensos", "color": "#2e7d32", "show": False},
    "descenso": {"name": "Flujos descensos", "color": "#c62828", "show": False},
    "sobrevuelo": {"name": "Flujos sobrevuelos", "color": "#1565c0", "show": False},
}
FLOW_BASE_WEIGHT = 3.0
FLOW_MAX_WEIGHT = 20.0
FLOW_MAX_LINES = 9
FLOW_BIN_SIZE_DEG = 7.5
FLOW_CLUSTER_SUPPRESS_DEG = 15.0
FLOW_MIN_COUNT = 10
FLOW_WEIGHT_EXP = 1.35
TMA_DEFINITIONS = [
    {"icao": "LEBL", "name": "TMA Barcelona", "radius_nm": 45.0},
    {"icao": "LEGE", "name": "TMA Girona", "radius_nm": 25.0},
    {"icao": "LERS", "name": "TMA Reus", "radius_nm": 25.0},
]

ICAO_REGION_COLORS = {
    "A": "#d32f2f",
    "B": "#c2185b",
    "C": "#7b1fa2",
    "D": "#512da8",
    "E": "#303f9f",
    "F": "#1976d2",
    "G": "#0288d1",
    "H": "#0097a7",
    "I": "#00796b",
    "J": "#388e3c",
    "K": "#689f38",
    "L": "#afb42b",
    "M": "#fbc02d",
    "N": "#ff8f00",
    "O": "#f57c00",
    "P": "#e64a19",
    "Q": "#5d4037",
    "R": "#616161",
    "S": "#455a64",
    "T": "#8d6e63",
    "U": "#c62828",
    "V": "#ad1457",
    "W": "#6a1b9a",
    "X": "#283593",
    "Y": "#0277bd",
    "Z": "#00695c",
}


ICAO_PREFIXES_PATH = Path("./icao_prefijos_pais_region.xlsx")

ICAO_REGION_LABELS = {
    "A": "África del Norte",
    "B": "Europa Occidental",
    "C": "Canadá",
    "D": "Oriente Medio",
    "E": "Europa del Norte",
    "F": "África Occidental",
    "G": "África Occidental",
    "H": "África Oriental",
    "I": "Asia Meridional",
    "J": "Caribe",
    "K": "Estados Unidos",
    "L": "Europa Mediterránea",
    "M": "Centroamérica",
    "N": "Pacífico Norte",
    "O": "Golfo Pérsico",
    "P": "Pacífico Central",
    "Q": "Atlántico Norte",
    "R": "Extremo Oriente",
    "S": "Sudamérica",
    "T": "Caribe / Atlántico",
    "U": "Rusia / CEI",
    "V": "Sudeste Asiático",
    "W": "Indonesia / Malasia",
    "X": "Europa Oriental",
    "Y": "Australia",
    "Z": "China",
}


def normalize_text(value: str) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", str(value))
    return "".join(ch for ch in value if not unicodedata.combining(ch)).lower().strip()


_ISO_NAME_CACHE = None

COUNTRY_CODE_HINTS = {
    "GF": ["guayana"],
    "GP": ["guadalupe"],
    "MQ": ["martinic"],
    "PM": ["miquelon"],
    "RE": ["reunion"],
    "YT": ["mayotte"],
}


def classify_altitude_change(entry_fl, exit_fl, threshold_ft: float = ALTITUDE_THRESHOLD_FT):
    """Clasifica el cambio de altitud entre entrada y salida en el sector."""
    entry = to_float(entry_fl)
    exit_ = to_float(exit_fl)
    if entry is None or exit_ is None:
        return None, None
    delta_ft = (exit_ - entry) * 100.0
    if math.isnan(delta_ft):
        return None, None
    if delta_ft > threshold_ft:
        return "ascenso", delta_ft
    if delta_ft < -threshold_ft:
        return "descenso", delta_ft
    return "sobrevuelo", delta_ft


def get_iso_official_name(country_code: str) -> str:
    global _ISO_NAME_CACHE
    code = (country_code or "").strip().upper()
    if not code:
        return ""
    if _ISO_NAME_CACHE is None:
        _ISO_NAME_CACHE = {}
        path = WORLD_GEOJSON_PATH
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                data = None
            if isinstance(data, dict):
                for feature in data.get("features", []):
                    props = feature.get("properties") or {}
                    iso = str(
                        props.get("iso_a2")
                        or props.get("ISO_A2")
                        or props.get("ISO2")
                        or props.get("COUNTRYAFF")
                        or ""
                    ).strip().upper()
                    if not iso:
                        continue
                    name_val = (
                        props.get("admin")
                        or props.get("ADMIN")
                        or props.get("name")
                        or props.get("NAME")
                        or props.get("NAME_LONG")
                    )
                    if name_val and iso not in _ISO_NAME_CACHE:
                        _ISO_NAME_CACHE[iso] = name_val
    return _ISO_NAME_CACHE.get(code, "")


def select_prefix_candidate(prefix_entry, country_name, country_code, hint_text=None):
    if not prefix_entry:
        return None, None
    candidates = prefix_entry if isinstance(prefix_entry, list) else [prefix_entry]
    rec_norm = normalize_text(country_name)
    hint_norm = normalize_text(hint_text)
    country_code = (country_code or "").strip().upper()
    official_hint = normalize_text(get_iso_official_name(country_code)) if country_code else ""
    best_data = None
    best_score = -1.0
    code_hints = COUNTRY_CODE_HINTS.get(country_code, ())

    for candidate in candidates:
        if isinstance(candidate, dict):
            cand_region = candidate.get("region")
            cand_name = candidate.get("country_name")
        else:
            cand_region = candidate
            cand_name = None
        cand_norm = normalize_text(cand_name)
        score = 0.0
        if rec_norm and cand_norm:
            score = SequenceMatcher(None, rec_norm, cand_norm).ratio()
            if cand_norm.startswith(rec_norm) or rec_norm.startswith(cand_norm):
                score += 0.2
            if cand_norm == rec_norm:
                score += 0.3
        elif cand_norm:
            score = 0.2
        if hint_norm and cand_norm and cand_norm in hint_norm:
            score += 0.25
        if cand_norm and code_hints:
            if any(hint in cand_norm for hint in code_hints):
                score += 0.3
        if official_hint and cand_norm:
            similarity = SequenceMatcher(None, official_hint, cand_norm).ratio()
            if similarity:
                score += 0.4 * similarity
        if country_code and cand_name and country_code in cand_name.upper():
            score += 0.1
        if best_data is None or score > best_score:
            best_score = score
            best_data = (cand_region, cand_name)

    if best_data is not None:
        return best_data

    fallback = candidates[0]
    if isinstance(fallback, dict):
        return fallback.get("region"), fallback.get("country_name")
    return fallback, None


def lighten_hex(color: str, factor: float = 0.4) -> str:
    color = color.lstrip("#")
    if len(color) != 6:
        return "#9e9e9e"
    r = int(color[0:2], 16)
    g = int(color[2:4], 16)
    b = int(color[4:6], 16)
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f"#{r:02x}{g:02x}{b:02x}"


def build_country_region_map(csv_path: Path):
    try:
        df = pd.read_csv(csv_path, usecols=["country_code", "region_name"])
    except Exception:
        return {}
    df = df.dropna(subset=["country_code", "region_name"])
    if df.empty:
        return {}
    grouped = df.groupby(df["country_code"].str.upper().str.strip()).agg({"region_name": "first"})
    return grouped["region_name"].to_dict()


def load_prefix_region_map(path: Path):
    if not path.exists():
        return {}
    try:
        df = pd.read_excel(path)
    except Exception as exc:
        print(f"⚠️ No se pudo leer {path}: {exc}")
        return {}
    prefix_map = {}
    for _, row in df.iterrows():
        prefix = str(row.get("prefijo") or "").strip().upper()
        region = str(row.get("región") or row.get("region") or "").strip()
        country_name = str(row.get("país") or row.get("pais") or "").strip()
        if not prefix or not region:
            continue
        data = {
            "region": region,
            "country_name": country_name or None,
        }
        existing = prefix_map.get(prefix)
        if not existing:
            prefix_map[prefix] = data
        else:
            if isinstance(existing, list):
                if data not in existing:
                    existing.append(data)
            else:
                if data != existing:
                    prefix_map[prefix] = [existing, data]
    return prefix_map


def load_world_geojson(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    try:
        resp = requests.get(WORLD_GEOJSON_URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        try:
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
        return data
    except Exception as exc:
        print(f"⚠️ No se pudo descargar el GeoJSON mundial: {exc}")
        return None


def build_country_region_map_from_index(index, prefix_region_map, fallback_map=None):
    info = {}

    for rec in index.values():
        cc = str(rec.get("country_code") or "").strip().upper()
        if not cc:
            continue
        entry = info.setdefault(cc, {
            "region": None,
            "letter": None,
            "name": rec.get("country_name"),
            "prefixes": set(),
            "_raw_prefixes": set(),
            "_prefix_country": {},
            "_prefix_region": {},
        })
        icao_code = rec.get("icao") or ""
        prefix = icao_code[:2] if len(icao_code) >= 2 else None
        base_letter = rec.get("region_letter")
        base_region = rec.get("region")
        country_name = rec.get("country_name")

        if country_name and not entry["name"]:
            entry["name"] = country_name
        if base_region and not entry["region"]:
            entry["region"] = base_region

        if prefix:
            entry["_raw_prefixes"].add(prefix)
            prefix_info = prefix_region_map.get(prefix) if prefix_region_map else None
            pref_region, pref_country = select_prefix_candidate(prefix_info, country_name, cc, rec.get("name"))

            if pref_region:
                entry.setdefault("_prefix_region", {}).setdefault(prefix, set()).add(pref_region)
                if not entry["region"]:
                    entry["region"] = pref_region
            elif base_region and not entry["region"]:
                entry["region"] = base_region

            if pref_country:
                entry.setdefault("_prefix_country", {}).setdefault(prefix, set()).add(pref_country)
            elif country_name and not entry["name"]:
                entry["name"] = country_name

            if not base_letter:
                base_letter = prefix[0]

        if base_letter and (entry["letter"] is None or entry["letter"] == "?"):
            entry["letter"] = base_letter

        if base_region and not entry["region"]:
            entry["region"] = base_region

    for cc, entry in info.items():
        prefix_country_map = entry.get("_prefix_country", {})
        raw_prefixes = entry.get("_raw_prefixes", set())

        country_candidates = []
        for names in prefix_country_map.values():
            if not names:
                continue
            if isinstance(names, set):
                country_candidates.extend(name for name in names if name)
            else:
                country_candidates.append(names)
        if country_candidates:
            most_common = Counter(country_candidates).most_common(1)[0][0]
            entry["name"] = most_common

        validated_prefixes = set()
        for prefix in raw_prefixes:
            mapped_names = prefix_country_map.get(prefix)
            if isinstance(mapped_names, set) and mapped_names and entry.get("name"):
                norm_target = normalize_text(entry["name"])
                for candidate in mapped_names:
                    if normalize_text(candidate) == norm_target:
                        validated_prefixes.add(prefix)
                        break
            elif not mapped_names:
                validated_prefixes.add(prefix)

        if not validated_prefixes and raw_prefixes:
            validated_prefixes = set(raw_prefixes)
        entry["prefixes"] = validated_prefixes

        prefix_region_map_entry = entry.get("_prefix_region", {})
        region_candidates = []
        for prefix in validated_prefixes:
            regions = prefix_region_map_entry.get(prefix)
            if isinstance(regions, set):
                region_candidates.extend(regions)
            elif regions:
                region_candidates.append(regions)
        if not region_candidates:
            for regions in prefix_region_map_entry.values():
                if isinstance(regions, set):
                    region_candidates.extend(regions)
                elif regions:
                    region_candidates.append(regions)
        if region_candidates:
            entry["region"] = Counter(region_candidates).most_common(1)[0][0]

        if (not entry["region"] or entry["region"] == "Desconocido") and fallback_map:
            fallback_region = fallback_map.get(cc)
            if fallback_region:
                entry["region"] = fallback_region
        if not entry["region"] and entry["letter"]:
            entry["region"] = ICAO_REGION_LABELS.get(entry["letter"], entry["letter"])
        if not entry["name"]:
            entry["name"] = cc

        if (entry.get("letter") in (None, "?") or not entry.get("letter")) and validated_prefixes:
            entry["letter"] = next(iter(sorted(validated_prefixes)))[0]

        entry.pop("_raw_prefixes", None)
        entry.pop("_prefix_country", None)
        entry.pop("_prefix_region", None)
    return info


OPENSKY_TRACK_URL = "https://opensky-network.org/api/tracks/all"


def sanitize_callsign(callsign: str) -> str:
    if not isinstance(callsign, str):
        return ""
    return "".join(ch for ch in callsign.upper().strip() if ch.isalnum())


def combine_date_time(date_value, time_value):
    try:
        date_dt = pd.to_datetime(date_value).to_pydatetime()
    except Exception:
        return None
    if pd.isna(date_value):
        return None
    if isinstance(time_value, pd.Timestamp):
        if pd.isna(time_value):
            return None
        t = time_value.time()
    else:
        time_str = str(time_value).strip()
        if not time_str:
            return None
        time_dt = pd.to_datetime(time_str, format="%H:%M:%S", errors="coerce")
        if pd.isna(time_dt):
            time_dt = pd.to_datetime(time_str, errors="coerce")
            if pd.isna(time_dt):
                return None
        t = time_dt.time()
    return datetime.combine(date_dt.date(), t)


def fetch_opensky_track(callsign: str, timestamp: int, auth, cache_dir: Path):
    if not callsign or timestamp is None or not auth:
        return None, False
    sanitized = sanitize_callsign(callsign)
    if not sanitized:
        return None, False
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{sanitized}_{int(timestamp)}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8")), True
        except Exception:
            pass
    params = {"callsign": sanitized, "time": str(int(timestamp))}
    try:
        resp = requests.get(OPENSKY_TRACK_URL, params=params, auth=auth, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            try:
                cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            except Exception:
                pass
            return data, False
        if resp.status_code != 404:
            print(f"⚠️ OpenSky respondió {resp.status_code} para {sanitized}")
    except Exception as exc:
        print(f"⚠️ Error consultando OpenSky ({sanitized}): {exc}")
    return None, False


def extract_track_points(track_data, ref_timestamp: int, margin_seconds: int):
    path = track_data.get("path") if isinstance(track_data, dict) else None
    if not path:
        return []
    points = []
    ref_ts = int(ref_timestamp) if ref_timestamp else None
    for item in path:
        lat = item.get("latitude")
        lon = item.get("longitude")
        if lat is None or lon is None:
            continue
        ts = item.get("time")
        if ref_ts is not None and margin_seconds and ts is not None:
            if abs(int(ts) - ref_ts) > margin_seconds:
                continue
        lat = float(lat)
        lon = float(lon)
        if points and abs(points[-1][0] - lat) <= 1e-5 and abs(points[-1][1] - lon) <= 1e-5:
            continue
        points.append((lat, lon))
    if len(points) < 2:
        points = []
        for item in path:
            lat = item.get("latitude")
            lon = item.get("longitude")
            if lat is None or lon is None:
                continue
            lat = float(lat)
            lon = float(lon)
            if points and abs(points[-1][0] - lat) <= 1e-5 and abs(points[-1][1] - lon) <= 1e-5:
                continue
            points.append((lat, lon))
    if len(points) > 400:
        step = len(points) // 400 + 1
        points = points[::step] + [points[-1]]
    return points


def macro_region(code: str):
    if not isinstance(code, str) or len(code) < 2:
        return "Desconocido"
    icao = code.strip().upper()
    prefix = icao[:2]
    first = icao[0]
    if first in ["O", "R", "V", "W", "Z"]:
        return "Asia"
    if first in ["K", "C", "M", "P", "T"] and prefix not in {
        "SA","SB","SC","SD","SE","SF","SG","SK","SL","SM","SN","SO","SP","SS","SU","SV","SW","SY"
    }:
        return "América del Norte"
    if prefix in {
        "SA","SB","SC","SD","SE","SF","SG","SK","SL","SM","SN","SO","SP","SS","SU","SV","SW","SY"
    }:
        return "América del Sur"
    if first in ["D", "F", "G", "H"]:
        return "África"
    if first in ["L", "E", "B"]:
        if prefix == "LP":
            return "Europa - Portugal"
        if prefix in {
            "LE","LF","EB","ED","EH","EG","EK","EN","EI","BI","EL","LO","LS","LX","ET"
        }:
            return "Europa - Norte/Francia"
        return "Europa - Este/Mediterráneo"
    return "Desconocido"


def haversine_nm(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * KM_TO_NM * c


def initial_bearing(lat1, lon1, lat2, lon2):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    x = math.sin(dlam) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    if abs(x) < 1e-12 and abs(y) < 1e-12:
        return 0.0
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360.0) % 360.0


def destination_point(lat, lon, bearing_deg, distance_nm):
    if distance_nm <= 0:
        return lat, lon
    angular_distance = (distance_nm / KM_TO_NM) / EARTH_RADIUS_KM
    bearing = math.radians(bearing_deg)
    phi1 = math.radians(lat)
    lam1 = math.radians(lon)
    sin_phi1 = math.sin(phi1)
    cos_phi1 = math.cos(phi1)
    sin_ad = math.sin(angular_distance)
    cos_ad = math.cos(angular_distance)
    sin_phi2 = sin_phi1 * cos_ad + cos_phi1 * sin_ad * math.cos(bearing)
    phi2 = math.asin(max(-1.0, min(1.0, sin_phi2)))
    y = math.sin(bearing) * sin_ad * cos_phi1
    x = cos_ad - sin_phi1 * sin_phi2
    lam2 = lam1 + math.atan2(y, x)
    return math.degrees(phi2), (math.degrees(lam2) + 540.0) % 360.0 - 180.0


def circle_polygon(lat, lon, radius_nm, segments=72):
    points = []
    for angle in range(0, 361, max(1, int(360 / segments))):
        pt_lat, pt_lon = destination_point(lat, lon, angle, radius_nm)
        points.append((pt_lat, pt_lon))
    if points and points[0] != points[-1]:
        points.append(points[0])
    return points


def slerp(lat1, lon1, lat2, lon2, fraction):
    if fraction <= 0:
        return lat1, lon1
    if fraction >= 1:
        return lat2, lon2
    phi1, lam1, phi2, lam2 = map(math.radians, (lat1, lon1, lat2, lon2))
    delta = 2 * math.asin(math.sqrt(math.sin((phi2 - phi1) / 2) ** 2 +
                                    math.cos(phi1) * math.cos(phi2) * math.sin((lam2 - lam1) / 2) ** 2))
    if delta == 0:
        return lat1, lon1
    a = math.sin((1 - fraction) * delta) / math.sin(delta)
    b = math.sin(fraction * delta) / math.sin(delta)
    x = a * math.cos(phi1) * math.cos(lam1) + b * math.cos(phi2) * math.cos(lam2)
    y = a * math.cos(phi1) * math.sin(lam1) + b * math.cos(phi2) * math.sin(lam2)
    z = a * math.sin(phi1) + b * math.sin(phi2)
    phi = math.atan2(z, math.sqrt(x * x + y * y))
    lam = math.atan2(y, x)
    return math.degrees(phi), math.degrees(lam)


def great_circle_points(lat1, lon1, lat2, lon2, samples=32):
    if samples <= 2:
        return [(lat1, lon1), (lat2, lon2)]
    return [slerp(lat1, lon1, lat2, lon2, i / (samples - 1)) for i in range(samples)]


def average_bearing(bearings):
    if not bearings:
        return 0.0
    sin_sum = sum(math.sin(math.radians(b)) for b in bearings)
    cos_sum = sum(math.cos(math.radians(b)) for b in bearings)
    if abs(sin_sum) < 1e-9 and abs(cos_sum) < 1e-9:
        return bearings[0]
    return (math.degrees(math.atan2(sin_sum, cos_sum)) + 360.0) % 360.0


def angular_distance_deg(a, b):
    diff = (a - b + 180.0) % 360.0 - 180.0
    return abs(diff)


def cluster_flow_samples(samples, max_lines, bin_size_deg=FLOW_BIN_SIZE_DEG, suppress_deg=FLOW_CLUSTER_SUPPRESS_DEG):
    if not samples:
        return []
    if max_lines <= 0:
        return []
    bin_size = max(1e-3, float(bin_size_deg))
    num_bins = max(1, int(round(360.0 / bin_size)))
    bin_samples = [[] for _ in range(num_bins)]
    for sample in samples:
        bearing = sample["bearing"] % 360.0
        idx = int(round(bearing / bin_size)) % num_bins
        bin_samples[idx].append(sample)
    counts = [len(bucket) for bucket in bin_samples]
    suppressed = [False] * num_bins
    selected_bins = []
    suppress_steps = max(0, int(math.ceil(suppress_deg / bin_size)))

    for _ in range(min(max_lines, num_bins)):
        candidate = max(range(num_bins), key=lambda i: counts[i] if not suppressed[i] else -1)
        if counts[candidate] <= 0 or suppressed[candidate]:
            break
        selected_bins.append(candidate)
        for offset in range(-suppress_steps, suppress_steps + 1):
            suppressed[(candidate + offset) % num_bins] = True

    if not selected_bins:
        candidate = max(range(num_bins), key=lambda i: counts[i])
        if counts[candidate] > 0:
            selected_bins = [candidate]
        else:
            return []

    centers = {idx: (idx * bin_size) % 360.0 for idx in selected_bins}
    clusters = {idx: [] for idx in selected_bins}

    for sample in samples:
        bearing = sample["bearing"]
        best_idx = min(selected_bins, key=lambda idx: angular_distance_deg(bearing, centers[idx]))
        clusters[best_idx].append(sample)

    cluster_list = []
    for idx in selected_bins:
        items = clusters.get(idx) or []
        if not items:
            continue
        count = len(items)
        avg_entry_lat = sum(s["entry"][0] for s in items) / count
        avg_entry_lon = sum(s["entry"][1] for s in items) / count
        avg_bearing_val = average_bearing([s["bearing"] for s in items])
        avg_length_nm = sum(s["length"] for s in items) / count
        spread = max((angular_distance_deg(s["bearing"], avg_bearing_val) for s in items), default=0.0)
        cluster_list.append({
            "count": count,
            "entry": (avg_entry_lat, avg_entry_lon),
            "bearing": avg_bearing_val,
            "length": avg_length_nm,
            "spread": spread,
        })

    cluster_list.sort(key=lambda item: item["count"], reverse=True)
    return cluster_list


def _points_equal(a, b, tol=1e-6):
    return abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol


def segment_intersection(p1, p2, q1, q2):
    x1, y1 = p1[1], p1[0]
    x2, y2 = p2[1], p2[0]
    x3, y3 = q1[1], q1[0]
    x4, y4 = q2[1], q2[0]
    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denom) < 1e-12:
        return None
    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
    u = ((x1 - x3) * (y1 - y2) - (y1 - y3) * (x1 - x2)) / denom
    if t < -1e-9 or t > 1 + 1e-9 or u < -1e-9 or u > 1 + 1e-9:
        return None
    t = max(0.0, min(1.0, t))
    xi = x1 + t * (x2 - x1)
    yi = y1 + t * (y2 - y1)
    return (yi, xi, t)


def segment_polygon_intersections(p0, p1, polygons):
    intersections = {}
    for poly in polygons:
        for ring in poly:
            if len(ring) < 2:
                continue
            for i in range(1, len(ring)):
                q0 = ring[i - 1]
                q1 = ring[i]
                inter = segment_intersection(p0, p1, q0, q1)
                if inter is None:
                    continue
                lat, lon, t = inter
                key = (round(lat, 8), round(lon, 8))
                if key not in intersections or t < intersections[key][0]:
                    intersections[key] = (t, (lat, lon))
    points = [(t, pt) for t, pt in intersections.values() if 0.0 <= t <= 1.0]
    points.sort(key=lambda item: item[0])
    return points


def clip_path_with_polygons(full_path, polygons):
    spans = []
    current = []
    if not polygons:
        return spans

    for idx in range(len(full_path) - 1):
        p0 = full_path[idx]
        p1 = full_path[idx + 1]
        intersections = segment_polygon_intersections(p0, p1, polygons)
        points = [(0.0, p0)]
        points.extend((t, pt) for t, pt in intersections if 0.0 < t < 1.0 and not _points_equal(pt, p0) and not _points_equal(pt, p1))
        points.append((1.0, p1))
        points.sort(key=lambda item: item[0])

        for i in range(len(points) - 1):
            start = points[i][1]
            end = points[i + 1][1]
            mid_lat = (start[0] + end[0]) / 2.0
            mid_lon = (start[1] + end[1]) / 2.0
            if point_in_any_polygon(mid_lat, mid_lon, polygons):
                if not current:
                    current.append(start)
                elif not _points_equal(current[-1], start):
                    current.append(start)
                current.append(end)
            else:
                if current:
                    spans.append(current)
                    current = []
    if current:
        spans.append(current)
    return spans


def ray_polygon_intersection(origin, target, polygons):
    if target is None:
        return None
    lat_o, lon_o = origin
    lat_t, lon_t = target
    dlat = lat_t - lat_o
    dlon = lon_t - lon_o
    if abs(dlat) < 1e-9 and abs(dlon) < 1e-9:
        dlat = 1e-4
    far_lat = lat_o + dlat * 100.0
    far_lon = lon_o + dlon * 100.0
    intersections = segment_polygon_intersections((lat_o, lon_o), (far_lat, far_lon), polygons)
    if not intersections:
        return None
    return intersections[-1][1]


def load_sector_polygons(cache_path: Path, refresh: bool = False):
    def ring_area_centroid(ring):
        if len(ring) < 3:
            return 0.0, (ring[0] if ring else (0.0, 0.0))
        area2 = 0.0
        cx = 0.0
        cy = 0.0
        for i in range(len(ring) - 1):
            lat1, lon1 = ring[i]
            lat2, lon2 = ring[i + 1]
            x1, y1 = lon1, lat1
            x2, y2 = lon2, lat2
            cross = x1 * y2 - x2 * y1
            area2 += cross
            cx += (x1 + x2) * cross
            cy += (y1 + y2) * cross
        area = area2 / 2.0
        if abs(area) < 1e-9:
            lat_avg = sum(pt[0] for pt in ring) / len(ring)
            lon_avg = sum(pt[1] for pt in ring) / len(ring)
            return 0.0, (lat_avg, lon_avg)
        cx = cx / (3.0 * area2)
        cy = cy / (3.0 * area2)
        return abs(area), (cy, cx)

    def compute_centroid(polygons):
        best_area = -1.0
        best_centroid = None
        for poly in polygons:
            if not poly.get("rings"):
                continue
            area, centroid = ring_area_centroid(poly["rings"][0])
            if area > best_area:
                best_area = area
                best_centroid = centroid
        return best_centroid

    def read_cache(path: Path):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            polygons = data.get("polygons") or []
            centroid = data.get("centroid")
            if centroid is None and polygons:
                centroid = compute_centroid(polygons)
                try:
                    data["centroid"] = centroid
                    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
            return polygons, centroid
        except Exception:
            return [], None

    if not refresh and cache_path.exists():
        polygons, centroid = read_cache(cache_path)
        if polygons:
            return polygons, centroid, True

    params = {
        "where": "DEPENDENCIA_CODE='BARCELONA RutaE'",
        "outFields": "IDENT_TXT,NOMBRE_TXT",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "pjson",
    }
    payload = None
    try:
        resp = requests.get(SECTOR_SERVICE_URL, params=params, timeout=45)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        payload = None

    if payload is None:
        query = urllib.parse.urlencode(params)
        url = f"{SECTOR_SERVICE_URL}?{query}"
        try:
            result = subprocess.run(["curl", "-s", url], capture_output=True, text=True, timeout=60, check=True)
            payload = json.loads(result.stdout)
        except Exception:
            payload = None

    if payload is None:
        polygons, centroid = read_cache(cache_path)
        return polygons, centroid, True if polygons else False

    polygons = []
    for feat in payload.get("features", []):
        attrs = feat.get("attributes") or {}
        geom = feat.get("geometry") or {}
        rings = []
        for ring in geom.get("rings") or []:
            converted = [(lat, lon) for lon, lat in ring if isinstance(lon, (int, float)) and isinstance(lat, (int, float))]
            if not converted:
                continue
            if converted[0] != converted[-1]:
                converted.append(converted[0])
            rings.append(converted)
        if rings:
            polygons.append({
                "ident": attrs.get("IDENT_TXT"),
                "name": attrs.get("NOMBRE_TXT"),
                "rings": rings,
            })

    if polygons:
        centroid = compute_centroid(polygons)
        try:
            cache_payload = {
                "fetched": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "source": SECTOR_SERVICE_URL,
                "polygons": polygons,
                "centroid": centroid,
            }
            cache_path.write_text(json.dumps(cache_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    return polygons, compute_centroid(polygons), False


def point_in_ring(lat: float, lon: float, ring):
    if len(ring) < 3:
        return False
    inside = False
    y = lat
    for i in range(len(ring)):
        lat1, lon1 = ring[i - 1]
        lat2, lon2 = ring[i]
        if ((lat1 > y) != (lat2 > y)):
            try:
                x_cross = (lon2 - lon1) * (y - lat1) / (lat2 - lat1) + lon1
            except ZeroDivisionError:
                x_cross = lon1
            if lon < x_cross:
                inside = not inside
    return inside


def point_in_polygon(lat: float, lon: float, polygon):
    if not polygon:
        return False
    if not point_in_ring(lat, lon, polygon[0]):
        return False
    for hole in polygon[1:]:
        if point_in_ring(lat, lon, hole):
            return False
    return True


def point_in_any_polygon(lat: float, lon: float, polygons):
    for poly in polygons:
        if point_in_polygon(lat, lon, poly.get("rings") if isinstance(poly, dict) else poly):
            return True
    return False


def accumulate_nm(points):
    total = 0.0
    for (lat1, lon1), (lat2, lon2) in zip(points, points[1:]):
        total += haversine_nm(lat1, lon1, lat2, lon2)
    return total


def as_folium_points(points):
    return [[lat, lon] for lat, lon in points]


def add_region_panel(map_obj, counts, colors):
    rows = "".join(
        f"<tr><td style='padding:2px 8px 2px 0;'>"
        f"<span style='display:inline-block;width:12px;height:12px;background:{colors.get(region, '#666')};"
        f"margin-right:6px;border-radius:2px;vertical-align:middle;'></span>{region}</td>"
        f"<td style='padding:2px 0;text-align:right;'>{count}</td></tr>"
        for region, count in counts.items()
    ) or "<tr><td colspan='2'>Sin datos</td></tr>"

    legend_html = (
        "<div class='icao-legend'><strong>Entradas por región ICAO</strong>"
        "<table>" + rows + "</table></div>"
    )

    template = Template("""
    {% macro script(this, kwargs) %}
    var legend_html = {{ this.legend_html|tojson }};
    var legend = L.control({position: 'bottomleft'});
    legend.onAdd = function(map) {
        var div = L.DomUtil.create('div', 'icao-legend-wrapper');
        div.innerHTML = legend_html;
        return div;
    };
    legend.addTo({{ this._parent.get_name() }});
    {% endmacro %}
    {% macro html(this, kwargs) %}
    <style>
    .icao-legend-wrapper {
        background: rgba(255,255,255,0.94);
        padding: 12px 16px;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.25);
        font-size: 13px;
        line-height: 1.35;
    }
    .icao-legend-wrapper table {
        margin-top: 6px;
        border-collapse: collapse;
    }
    .icao-legend-wrapper table td {
        padding: 2px;
    }
    </style>
    {% endmacro %}
    """)

    macro = MacroElement()
    macro._template = template
    macro.legend_html = legend_html
    map_obj.get_root().add_child(macro)


def ensure_matplotlib():
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt  # noqa: F401
        return True
    except Exception:
        print("⚠️ matplotlib.pyplot no disponible; se omiten gráficos analíticos.")
        return False


def plot_bar(series, title, xlabel, ylabel, outfile, color):
    import matplotlib.pyplot as plt

    if series.empty:
        return
    plt.figure(figsize=(10, 6))
    ax = series.plot(kind="bar", color=color)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    total = series.sum()
    for i, (label, value) in enumerate(series.items()):
        pct = (value / total) * 100 if total else 0
        ax.text(i, value + max(total * 0.01, 0.5), f"{value}\n{pct:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(outfile, dpi=160)
    plt.close()


def plot_hist_duration(df, outfile):
    import matplotlib.pyplot as plt
    df = df.copy()
    df["Entry time"] = pd.to_datetime(df["Entry time"], format="%H:%M:%S", errors="coerce")
    df["Exit time"] = pd.to_datetime(df["Exit time"], format="%H:%M:%S", errors="coerce")
    df["Duración_min"] = (df["Exit time"] - df["Entry time"]).dt.total_seconds() / 60
    df = df.dropna(subset=["Duración_min"])
    df = df[df["Duración_min"] > 0]
    if df.empty:
        return
    bins = range(0, int(df["Duración_min"].max()) + 10, 5)
    labels = [f"{i}-{i+5}" for i in bins[:-1]]
    df["Intervalo"] = pd.cut(df["Duración_min"], bins=bins, labels=labels, right=False)
    counts = df["Intervalo"].value_counts().sort_index()
    plt.figure(figsize=(10, 6))
    bars = plt.bar(counts.index.astype(str), counts.values, color="skyblue", edgecolor="black")
    plt.title("Distribución del tiempo en el espacio aéreo")
    plt.xlabel("Tiempo dentro del espacio aéreo (min)")
    plt.ylabel("Número de vuelos")
    plt.xticks(rotation=45, ha="right")
    for bar, value in zip(bars, counts.values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5, str(int(value)), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(outfile, dpi=160)
    plt.close()


def plot_altitude_types(df, outfile):
    import matplotlib.pyplot as plt
    if "Entry FL" not in df.columns or "Exit FL" not in df.columns:
        return
    df = df.copy()
    categories = []
    for entry_fl, exit_fl in zip(df["Entry FL"], df["Exit FL"]):
        category, _ = classify_altitude_change(entry_fl, exit_fl)
        categories.append(category)
    df["Alt_Category"] = categories
    valid_categories = [cat for cat in ALTITUDE_CATEGORY_LABELS.keys()]
    df = df[df["Alt_Category"].isin(valid_categories)]
    if df.empty:
        return
    counts = df["Alt_Category"].value_counts()
    counts = counts.reindex(valid_categories, fill_value=0)
    labels = [ALTITUDE_CATEGORY_LABELS[key] for key in counts.index]
    colors = ["#2e7d32", "#c62828", "#1565c0"]
    plt.figure(figsize=(8, 6))
    bars = plt.bar(labels, counts.values, color=colors, edgecolor="black")
    plt.title("Vuelos por perfil de altitud")
    plt.xlabel("Perfil")
    plt.ylabel("Número de vuelos")
    for bar, value in zip(bars, counts.values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5, str(int(value)), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(outfile, dpi=160)
    plt.close()


def generate_insights(entry_df, departures_df, panel_counts, args, region_label_fn):
    if not args.plots_dir:
        return
    if not ensure_matplotlib():
        return
    out_dir = Path(args.plots_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    entry_regions = entry_df["Origin"].dropna().apply(region_label_fn).value_counts()
    if args.region_threshold > 0:
        entry_regions = entry_regions[entry_regions > args.region_threshold]
    plot_bar(entry_regions, f"Regiones de entrada (> {args.region_threshold} vuelos)", "Región", "Número de vuelos", out_dir / "entradas_por_region.png", "#1976d2")

    dest_df = departures_df if departures_df is not None else entry_df
    destination_regions = dest_df["Destination"].dropna().apply(region_label_fn).value_counts()
    if args.region_threshold > 0:
        destination_regions = destination_regions[destination_regions > args.region_threshold]
    plot_bar(destination_regions, f"Regiones de destino (> {args.region_threshold} vuelos)", "Región", "Número de vuelos", out_dir / "destinos_por_region.png", "#c62828")

    entry_macro_counts = entry_df["Origin"].dropna().apply(macro_region).value_counts()
    plot_bar(entry_macro_counts, "Entradas por macro-región", "Macro-región", "Número de vuelos", out_dir / "entradas_macro.png", "#1976d2")

    dest_macro_counts = dest_df["Destination"].dropna().apply(macro_region).value_counts()
    plot_bar(dest_macro_counts, "Destinos por macro-región", "Macro-región", "Número de vuelos", out_dir / "destinos_macro.png", "#f57c00")

    plot_hist_duration(entry_df, out_dir / "tiempo_en_sector.png")
    plot_altitude_types(entry_df, out_dir / "tipos_altitud.png")

    outfile_panel = out_dir / "panel_regiones.csv"
    pd.Series(panel_counts).to_csv(outfile_panel, header=["Vuelos"], index_label="Región")

def norm_code(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = s.replace(" ", "").replace("-", "").replace("_", "")
    if 3 <= len(s) <= 4:
        return s
    return None

def fetch_airport_by_code(code: str, index: dict):
    if not code:
        return None
    return index.get(code.upper())

def airport_coords(record):
    if not record:
        return None
    lat = record.get("latitude")
    lon = record.get("longitude")
    name = record.get("name") or ""
    iata = record.get("iata") or ""
    icao = record.get("icao") or ""
    label = f"{name} [{icao or iata}]".strip()
    if lat is None or lon is None:
        return None
    return (float(lat), float(lon), label)

def detect_columns(df: pd.DataFrame, origin_col=None, dest_col=None):
    if origin_col and dest_col:
        return origin_col, dest_col
    candidates_o = ["ADEP","ORIGIN","ORIG","DEP","DEPARTURE","FROM","ADEP_ICAO","ADEP_IATA","Origin"]
    candidates_d = ["ADES","DESTINATION","DEST","ARR","ARRIVAL","TO","ADES_ICAO","ADES_IATA","Destination"]
    def find_col(cands):
        for cand in cands:
            for col in df.columns:
                if col.lower() == cand.lower():
                    return col
            for col in df.columns:
                if cand.lower() in col.lower():
                    return col
        return None
    oc = origin_col or find_col(candidates_o) or df.columns[0]
    dc = dest_col   or find_col(candidates_d) or df.columns[1]
    return oc, dc

def main():
    parser = argparse.ArgumentParser(description="Traza rutas desde Excel usando un índice local de aeropuertos.")
    parser.add_argument("excel_path", type=str, help="Ruta al Excel de entradas de vuelo")
    parser.add_argument("--origin-col", type=str, default=None, help="Nombre de columna de origen (ADEP/IATA/ICAO)")
    parser.add_argument("--dest-col", type=str, default=None, help="Nombre de columna de destino (ADES/IATA/ICAO)")
    parser.add_argument("--out", type=str, default="red_rutas.html", help="Archivo HTML de salida")
    parser.add_argument("--airports-csv", type=str, default="iata-icao.csv", help="Ruta al CSV con aeropuertos (IATA/ICAO)")
    parser.add_argument("--sector-geojson", type=str, default=str(SECTOR_CACHE_PATH), help="Cache local de la geometría LECB E")
    parser.add_argument("--refresh-sector", action="store_true", help="Forzar descarga de la geometría LECB E desde ENAIRE")
    parser.add_argument("--sector-center-lat", type=float, default=41.3, help="Latitud aproximada del centro del sector (respaldo)")
    parser.add_argument("--sector-center-lon", type=float, default=2.3, help="Longitud aproximada del centro del sector (respaldo)")
    parser.add_argument("--route-samples", type=int, default=96, help="Número de puntos para interpolar la ruta completa")
    parser.add_argument("--sector-samples", type=int, default=24, help="Número de puntos para la porción dentro del sector (respaldo)")
    parser.add_argument("--cross-fallback", type=float, default=0.25, help="Fracción del trayecto a usar si falta la longitud de cruce")
    parser.add_argument("--departures-excel", type=str, default=None, help="Excel adicional con vuelos de salida (para estadísticas)")
    parser.add_argument("--plots-dir", type=str, default=None, help="Directorio donde guardar gráficos analíticos opcionales")
    parser.add_argument("--region-threshold", type=int, default=10, help="Umbral mínimo de vuelos por región en los gráficos")
    parser.add_argument("--opensky-user", type=str, default=None, help="Usuario de OpenSky Network para recuperar trayectorias ADS-B")
    parser.add_argument("--opensky-password", type=str, default=None, help="Contraseña de OpenSky Network")
    parser.add_argument("--tracks-dir", type=str, default="opensky_tracks", help="Directorio de cache para pistas OpenSky")
    parser.add_argument("--track-margin-minutes", type=int, default=30, help="Margen en minutos alrededor de la hora de entrada para filtrar puntos de la trayectoria")
    parser.add_argument("--world-geojson", type=str, default=str(WORLD_GEOJSON_PATH), help="GeoJSON mundial para colorear regiones")
    parser.add_argument("--icao-prefixes", type=str, default=str(ICAO_PREFIXES_PATH), help="Tabla Excel con prefijos ICAO y regiones")

    args = parser.parse_args()
    print(f"Archivo Excel proporcionado: {args.excel_path}")

    excel = Path(args.excel_path)
    if not excel.exists():
        raise FileNotFoundError(f"No se encontró el Excel: {excel}")

    df = pd.read_excel(excel)
    departures_df = None
    if args.departures_excel:
        try:
            departures_df = pd.read_excel(Path(args.departures_excel))
        except Exception as exc:
            print(f"⚠️ No se pudo leer el Excel de salidas ({args.departures_excel}): {exc}")
    origin_col, dest_col = detect_columns(df, args.origin_col, args.dest_col)

    df_original = df.copy()
    df["_ORIGIN"] = df[origin_col].map(norm_code)
    df["_DEST"] = df[dest_col].map(norm_code)
    df = df.dropna(subset=["_ORIGIN","_DEST"]).copy()

    panel_counts = {}
    panel_colors = {}
    route_geojson_features = []
    category_geojson_features = {key: [] for key in ALTITUDE_CATEGORY_LABELS}
    flow_samples = {key: [] for key in ["all", *ALTITUDE_CATEGORY_LABELS.keys()]}

    sector_cache_path = Path(args.sector_geojson)
    sector_raw, sector_centroid, sector_from_cache = load_sector_polygons(sector_cache_path, refresh=args.refresh_sector)
    sector_polygons = [poly["rings"] for poly in sector_raw]
    if sector_polygons:
        origin_sector_msg = "cache local" if sector_from_cache else "servicio ENAIRE"
        print(f"Geometría LECB E cargada ({origin_sector_msg}): {len(sector_polygons)} polígonos")
    else:
        print("⚠️ No se pudo obtener la geometría oficial LECB E. Se aplicará la aproximación geométrica.")

    airports_csv_path = Path(args.airports_csv)
    prefix_region_map = load_prefix_region_map(Path(args.icao_prefixes)) if args.icao_prefixes else {}
    airport_index = load_airport_index(airports_csv_path, prefix_region_map)
    raw_country_map = build_country_region_map(airports_csv_path)
    country_region_map = build_country_region_map_from_index(airport_index, prefix_region_map, raw_country_map)
    world_geojson = load_world_geojson(Path(args.world_geojson)) if args.world_geojson else None

    opensky_auth = None
    tracks_dir = None
    if args.opensky_user and args.opensky_password:
        opensky_auth = (args.opensky_user, args.opensky_password)
        tracks_dir = Path(args.tracks_dir) if args.tracks_dir else Path("opensky_tracks")
        tracks_dir.mkdir(parents=True, exist_ok=True)
    track_margin_seconds = max(int(args.track_margin_minutes), 0) * 60
    track_stats = {"attempted": 0, "success": 0, "cached": 0}

    resolved = {}
    resolution_errors = {}

    def resolve_region_info(code: str):
        if not isinstance(code, str) or not code:
            return "Desconocido", "?"
        normalized = norm_code(code) or code.strip().upper()
        rec = airport_index.get(normalized)
        if not rec and len(normalized) == 4:
            rec = airport_index.get(normalized[:4])
        region_name = None
        region_letter = None
        if rec:
            region_name = rec.get("region")
            region_letter = rec.get("region_letter")
            if not region_name and rec.get("country_code"):
                info = country_region_map.get(str(rec.get("country_code")).strip().upper())
                if isinstance(info, tuple):
                    region_name, inferred_letter = info
                    region_letter = region_letter or inferred_letter
        if not region_letter and normalized:
            region_letter = normalized[0]
        if not region_name and region_letter:
            region_name = ICAO_REGION_LABELS.get(region_letter, region_letter)
        if not region_name:
            region_name = "Desconocido"
        if not region_letter:
            region_letter = "?"
        return region_name, region_letter

    def region_label(code: str):
        return resolve_region_info(code)[0]

    panel_counts = {}
    panel_colors = {}
    for code in df["_ORIGIN"].dropna():
        label, letter = resolve_region_info(code)
        panel_counts[label] = panel_counts.get(label, 0) + 1
        panel_colors.setdefault(label, ICAO_REGION_COLORS.get(letter, "#455a64"))

    def resolve(code: str):
        if not code:
            return None
        if code in resolved:
            return resolved[code]
        rec = fetch_airport_by_code(code, airport_index)
        if not rec:
            resolution_errors[code] = f"Código no encontrado en {args.airports_csv}"
        resolved[code] = rec
        return rec

    for code in pd.unique(pd.concat([df["_ORIGIN"], df["_DEST"]])):
        try:
            resolve(code)
        except Exception as exc:
            resolution_errors[code] = str(exc)

    def register_flow(category_key, entry_pt, bearing_deg, segment_length_nm):
        collection = flow_samples.get(category_key)
        if collection is None:
            return
        collection.append({
            "entry": entry_pt,
            "bearing": bearing_deg % 360.0,
            "length": segment_length_nm,
        })

    # Construir rutas con coordenadas
    routes = []
    unresolved = []
    for _, row in df.iterrows():
        o = row["_ORIGIN"]; d = row["_DEST"]
        o_rec = resolve(o); d_rec = resolve(d)
        o_geo = airport_coords(o_rec); d_geo = airport_coords(d_rec)
        if not o_geo or not d_geo:
            unresolved.append((o, d))
            continue

        region_name, region_letter = resolve_region_info(o)
        route_color = panel_colors.setdefault(
            region_name,
            ICAO_REGION_COLORS.get(region_letter, "#1976d2")
        )
        altitude_category, altitude_delta_ft = classify_altitude_change(row.get("Entry FL"), row.get("Exit FL"))
        if altitude_category is None:
            altitude_category = "sobrevuelo"
        altitude_label = ALTITUDE_CATEGORY_LABELS.get(altitude_category, altitude_category.capitalize())

        lat_o, lon_o, name_o = o_geo
        lat_d, lon_d, name_d = d_geo
        total_nm = haversine_nm(lat_o, lon_o, lat_d, lon_d)
        if not total_nm or math.isclose(total_nm, 0, abs_tol=1e-6):
            unresolved.append((o, d))
            continue

        raw_cross_len = row.get("Cross length (NM)")
        try:
            raw_cross_len = float(raw_cross_len)
        except (TypeError, ValueError):
            raw_cross_len = float("nan")

        track_path = None
        track_source = "Interpolación geodésica"
        entry_dt = combine_date_time(row.get("Date"), row.get("Entry time"))
        track_ts = int(entry_dt.timestamp()) if entry_dt else None
        callsign = row.get("Call Sign") or row.get("CallSign")
        if opensky_auth and tracks_dir and track_ts is not None and callsign:
            sanitized_call = sanitize_callsign(callsign)
            if sanitized_call:
                track_stats["attempted"] += 1
                track_data, cached_track = fetch_opensky_track(sanitized_call, track_ts, opensky_auth, tracks_dir)
                if track_data:
                    track_points = extract_track_points(track_data, track_ts, track_margin_seconds)
                    if track_points and len(track_points) >= 2:
                        track_path = track_points
                        track_source = "OpenSky (cache)" if cached_track else "OpenSky"
                        track_stats["success"] += 1
                        if cached_track:
                            track_stats["cached"] += 1

        dense_points = None if track_path else great_circle_points(lat_o, lon_o, lat_d, lon_d, max(args.route_samples, 8))
        full_path = track_path if track_path else (dense_points if dense_points else [(lat_o, lon_o), (lat_d, lon_d)])

        entry_point = None
        exit_point = None
        sector_path = None
        cross_len = raw_cross_len
        segment_method = "sector"

        if sector_polygons:
            spans = clip_path_with_polygons(full_path, sector_polygons)
            if spans:
                best_span = max(spans, key=accumulate_nm)
                sector_path = []
                for pt in best_span:
                    if not sector_path or not _points_equal(sector_path[-1], pt):
                        sector_path.append(pt)
                if len(sector_path) >= 2:
                    entry_point = sector_path[0]
                    exit_point = sector_path[-1]
                    cross_len = accumulate_nm(sector_path)
                else:
                    sector_path = None

        if sector_path is None:
            segment_method = "fallback"
            if sector_centroid and sector_polygons:
                centroid = sector_centroid
                entry_pt = ray_polygon_intersection(centroid, (lat_o, lon_o), sector_polygons)
                exit_pt = ray_polygon_intersection(centroid, (lat_d, lon_d), sector_polygons)
                if entry_pt and exit_pt and (abs(entry_pt[0] - exit_pt[0]) > 1e-6 or abs(entry_pt[1] - exit_pt[1]) > 1e-6):
                    synthetic_path = [(lat_o, lon_o)]
                    if not _points_equal(entry_pt, synthetic_path[-1]):
                        synthetic_path.append(entry_pt)
                    if not _points_equal(centroid, synthetic_path[-1]):
                        synthetic_path.append(centroid)
                    if not _points_equal(exit_pt, synthetic_path[-1]):
                        synthetic_path.append(exit_pt)
                    if not _points_equal((lat_d, lon_d), synthetic_path[-1]):
                        synthetic_path.append((lat_d, lon_d))
                    full_path = synthetic_path
                    sector_path = [entry_pt]
                    if not _points_equal(entry_pt, centroid):
                        sector_path.append(centroid)
                    if not _points_equal(centroid, exit_pt):
                        sector_path.append(exit_pt)
                    entry_point = entry_pt
                    exit_point = exit_pt
                    cross_len = accumulate_nm(sector_path)
                    segment_method = "synthetic_centroid"
            if sector_path is None:
                if math.isnan(cross_len) or cross_len <= 0:
                    cross_len = max(total_nm * args.cross_fallback, total_nm * 0.05)
                cross_len = min(cross_len, total_nm)

                center_lat = args.sector_center_lat
                center_lon = args.sector_center_lon
                if full_path:
                    distances = [haversine_nm(p[0], p[1], center_lat, center_lon) for p in full_path]
                    center_idx = min(range(len(full_path)), key=lambda i: distances[i])
                    center_fraction = center_idx / (len(full_path) - 1) if len(full_path) > 1 else 0.5
                else:
                    center_fraction = 0.5

                center_s = center_fraction * total_nm
                half_len = cross_len / 2
                entry_s = max(0.0, center_s - half_len)
                exit_s = min(total_nm, center_s + half_len)
                if exit_s <= entry_s:
                    entry_s = max(0.0, min(total_nm, center_s - 0.01 * total_nm))
                    exit_s = min(total_nm, max(entry_s + 0.02 * total_nm, center_s + 0.01 * total_nm))

                entry_fraction = entry_s / total_nm
                exit_fraction = exit_s / total_nm
                entry_point = slerp(lat_o, lon_o, lat_d, lon_d, entry_fraction)
                exit_point = slerp(lat_o, lon_o, lat_d, lon_d, exit_fraction)
                sector_path = great_circle_points(entry_point[0], entry_point[1], exit_point[0], exit_point[1], max(args.sector_samples, 4))
                cross_len = accumulate_nm(sector_path)
                entry_point = None
                exit_point = None

        flow_entry_pt = None
        flow_second_pt = None
        flow_path = sector_path if sector_path and len(sector_path) >= 2 else None
        if flow_path is None and full_path and len(full_path) >= 2:
            flow_path = full_path[:2]
        if flow_path and len(flow_path) >= 2:
            flow_entry_pt = flow_path[0]
            flow_second_pt = flow_path[1]
        if flow_entry_pt and flow_second_pt:
            flow_bearing = initial_bearing(flow_entry_pt[0], flow_entry_pt[1], flow_second_pt[0], flow_second_pt[1])
            flow_segment_len = haversine_nm(flow_entry_pt[0], flow_entry_pt[1], flow_second_pt[0], flow_second_pt[1])
            if flow_segment_len > 0:
                register_flow("all", flow_entry_pt, flow_bearing, flow_segment_len)
                register_flow(altitude_category, flow_entry_pt, flow_bearing, flow_segment_len)

        tooltip_text = f"{o} → {d} ({altitude_label})"
        popup_html = f"{name_o} → {name_d}<br>Trayecto total: {total_nm:.1f} NM"
        if track_source:
            popup_html += f"<br>Ruta: {track_source}"
        popup_html += f"<br>Región ICAO origen: {region_name}"
        if altitude_delta_ft is not None:
            popup_html += f"<br>Perfil: {altitude_label} ({altitude_delta_ft:+.0f} ft)"
        else:
            popup_html += f"<br>Perfil: {altitude_label}"

        default_weight = 1.6
        default_opacity = 0.65
        hover_weight = 3.0
        hover_opacity = 0.85
        selected_weight = 5.0
        selected_opacity = 0.95

        routes.append({
            "origin": o,
            "dest": d,
            "o_geo": o_geo,
            "d_geo": d_geo,
            "full_path": full_path,
            "sector_path": sector_path,
            "entry_point": entry_point,
            "exit_point": exit_point,
            "cross_len": cross_len,
            "total_nm": total_nm,
            "segment_method": segment_method,
            "region": region_name,
            "route_color": route_color,
            "track_source": track_source,
            "altitude_category": altitude_category,
            "altitude_delta_ft": altitude_delta_ft,
            "tooltip_text": tooltip_text,
            "popup_html": popup_html,
        })

        if len(full_path) >= 2:
            coords = [[pt[1], pt[0]] for pt in full_path]
            feature = {
                "type": "Feature",
                "properties": {
                    "route_id": len(route_geojson_features),
                    "color": route_color,
                    "default_weight": default_weight,
                    "default_opacity": default_opacity,
                    "hover_weight": hover_weight,
                    "hover_opacity": hover_opacity,
                    "selected_weight": selected_weight,
                    "selected_opacity": selected_opacity,
                    "tooltip_text": tooltip_text,
                    "popup_html": popup_html,
                    "altitude_category": altitude_label,
                    "altitude_category_key": altitude_category,
                    "altitude_delta_ft": altitude_delta_ft,
                },
                "geometry": {"type": "LineString", "coordinates": coords},
            }
            route_geojson_features.append(feature)
            if altitude_category in category_geojson_features:
                category_geojson_features[altitude_category].append(copy.deepcopy(feature))

    panel_series = pd.Series(panel_counts).sort_values(ascending=False)
    sector_layer = None
    tma_features = []
    if TMA_DEFINITIONS:
        for tma in TMA_DEFINITIONS:
            icao = tma.get("icao")
            if not icao:
                continue
            rec = fetch_airport_by_code(icao, airport_index)
            if not rec:
                continue
            lat = rec.get("latitude")
            lon = rec.get("longitude")
            if lat is None or lon is None:
                continue
            lat = float(lat)
            lon = float(lon)
            if sector_polygons and not point_in_any_polygon(lat, lon, sector_polygons):
                continue
            radius_nm = float(tma.get("radius_nm", 25.0))
            ring = circle_polygon(lat, lon, radius_nm, segments=96)
            if not ring:
                continue
            coords = [[pt_lon, pt_lat] for pt_lat, pt_lon in ring]
            props = {
                "name": tma.get("name") or icao,
                "icao": icao,
                "radius_nm": radius_nm,
            }
            tma_features.append({
                "type": "Feature",
                "properties": props,
                "geometry": {"type": "Polygon", "coordinates": [coords]},
            })

    # Mapa
    m = folium.Map(location=[43.0, 3.0], zoom_start=4, tiles="CartoDB positron")

    if world_geojson and country_region_map:
        region_layer = folium.FeatureGroup(name="Regiones ICAO", show=True)
        for feature in world_geojson.get("features", []):
            props = feature.get("properties") or {}
            iso = str(
                props.get("ISO_A2")
                or props.get("ISO2")
                or props.get("iso_a2")
                or props.get("COUNTRYAFF")
                or ""
            ).upper()
            if not iso:
                continue
            info = country_region_map.get(iso)
            if not info:
                continue
            region_name = info.get("region")
            region_letter = info.get("letter")
            if not region_name or region_name == "Desconocido":
                continue
            color = panel_colors.setdefault(region_name, ICAO_REGION_COLORS.get(region_letter, "#607d8b"))
            fill = lighten_hex(color, 0.55)
            country_name = info.get("name") or props.get("ADMIN") or props.get("NAME") or iso
            prefixes = ", ".join(sorted(p for p in info.get("prefixes", []) if p)) or "N/A"
            country_label = f"{country_name} ({prefixes})"
            display_feature = {
                "type": "Feature",
                "properties": {
                    "region": region_name,
                    "country": country_label,
                },
                "geometry": feature.get("geometry"),
            }
            folium.GeoJson(
                display_feature,
                style_function=lambda _, color=color, fill=fill: {
                    "color": color,
                    "fillColor": fill,
                    "weight": 0.8,
                    "fillOpacity": 0.28,
                },
                highlight_function=lambda _: {"weight": 2, "color": "#212121"},
                tooltip=folium.GeoJsonTooltip(fields=["country", "region"], aliases=["País", "Región"]),
            ).add_to(region_layer)
        region_layer.add_to(m)

    mc = MarkerCluster(name="Aeropuertos").add_to(m)


    if sector_polygons:
        multipoly = []
        for poly in sector_polygons:
            rings = []
            for ring in poly:
                rings.append([[lon, lat] for lat, lon in ring])
            multipoly.append(rings)
        sector_layer = folium.GeoJson(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": "ACC LECB E"},
                        "geometry": {"type": "MultiPolygon", "coordinates": multipoly},
                    }
                ],
            },
            style_function=lambda _: {
                "color": "#ffa000",
                "weight": 1.5,
                "fillOpacity": 0.05,
                "interactive": False,
            },
            name="ACC LECB E",
        )

    if tma_features:
        tma_layer = folium.FeatureGroup(name="TMAs", show=False)
        for feat in tma_features:
            base_color = "#00897b"
            fill_color = lighten_hex(base_color, 0.6)
            gj = folium.GeoJson(
                feat,
                style_function=lambda _, color=base_color, fill=fill_color: {
                    "color": color,
                    "weight": 1.2,
                    "dashArray": "6,4",
                    "fillColor": fill,
                    "fillOpacity": 0.15,
                },
                highlight_function=lambda _: {"weight": 2.0, "dashArray": "1", "fillOpacity": 0.25},
            )
            folium.features.GeoJsonTooltip(
                fields=["name", "radius_nm"],
                aliases=["TMA", "Radio (NM)"],
                sticky=True,
            ).add_to(gj)
            folium.features.GeoJsonPopup(
                fields=["name", "icao", "radius_nm"],
                aliases=["TMA", "Aeropuerto", "Radio (NM)"],
                labels=True,
            ).add_to(gj)
            gj.add_to(tma_layer)
        tma_layer.add_to(m)

    # Marcadores únicos
    added = set()
    for route in routes:
        for lat, lon, label in (route["o_geo"], route["d_geo"]):
            key = (lat, lon, label)
            if key in added: continue
            added.add(key)
            folium.Marker([lat, lon], tooltip=label, popup=label).add_to(mc)

    # Cruces e intersecciones omitidos en esta versión

    if route_geojson_features:
        highlight_template_str = """
        {% macro script(this, kwargs) %}
        var routesLayer = {{this._parent.get_name()}};
        routesLayer.eachLayer(function(layer){
            var props = layer.feature.properties;
            layer.setStyle({color: props.color, weight: props.default_weight, opacity: props.default_opacity});
            props._selected = false;
            layer.on('click', function(e){
                routesLayer.eachLayer(function(l){
                    var p = l.feature.properties;
                    p._selected = false;
                    l.setStyle({color: p.color, weight: p.default_weight, opacity: p.default_opacity});
                });
                props._selected = true;
                layer.setStyle({color: props.color, weight: props.selected_weight, opacity: props.selected_opacity});
                layer.bringToFront();
                if(layer.getPopup()){ layer.openPopup(); }
            });
            layer.on('mouseover', function(e){
                if (!props._selected){
                    layer.setStyle({color: props.color, weight: props.hover_weight, opacity: props.hover_opacity});
                    layer.bringToFront();
                }
            });
            layer.on('mouseout', function(e){
                if (!props._selected){
                    layer.setStyle({color: props.color, weight: props.default_weight, opacity: props.default_opacity});
                }
            });
        });
        {% endmacro %}
        """

        def add_routes_layer(features, layer_name, show_layer):
            if not features:
                return None
            layer = folium.GeoJson(
                {"type": "FeatureCollection", "features": features},
                name=layer_name,
                show=show_layer,
                style_function=lambda feat: {
                    "color": feat["properties"]["color"],
                    "weight": feat["properties"]["default_weight"],
                    "opacity": feat["properties"]["default_opacity"],
                },
                highlight_function=lambda feat: {
                    "color": feat["properties"]["color"],
                    "weight": feat["properties"]["hover_weight"],
                    "opacity": feat["properties"]["hover_opacity"],
                },
            )
            folium.features.GeoJsonTooltip(fields=["tooltip_text"], aliases=["Ruta"], sticky=True).add_to(layer)
            folium.features.GeoJsonPopup(fields=["popup_html"], labels=False, parse_html=True).add_to(layer)
            highlight_macro = MacroElement()
            highlight_macro._template = Template(highlight_template_str)
            layer.add_child(highlight_macro)
            layer.add_to(m)
            return layer

        add_routes_layer(route_geojson_features, "Rutas", False)
        for key, label in ALTITUDE_CATEGORY_LABELS.items():
            add_routes_layer(category_geojson_features.get(key), label, False)

    for category_key, style in FLOW_LAYER_STYLES.items():
        samples = flow_samples.get(category_key) or []
        clusters = cluster_flow_samples(samples, FLOW_MAX_LINES)
        if not clusters:
            continue
        candidate_counts = [cluster["count"] for cluster in clusters if cluster["count"] >= FLOW_MIN_COUNT]
        if not candidate_counts:
            continue
        max_count = max(candidate_counts)
        if max_count <= 0:
            continue
        flow_layer = folium.FeatureGroup(name=style["name"], show=style["show"])
        layer_has_data = False
        for cluster in clusters:
            count = cluster["count"]
            if count < FLOW_MIN_COUNT:
                continue
            avg_entry_lat, avg_entry_lon = cluster["entry"]
            avg_bearing_val = cluster["bearing"]
            avg_length = cluster["length"]
            if sector_centroid:
                end_lat, end_lon = sector_centroid
            else:
                extend_nm = max(avg_length * 4.0, 20.0)
                end_lat, end_lon = destination_point(avg_entry_lat, avg_entry_lon, avg_bearing_val, extend_nm)
            weight = FLOW_BASE_WEIGHT
            if max_count > 0:
                scale = (count / max_count) ** FLOW_WEIGHT_EXP
                weight += (FLOW_MAX_WEIGHT - FLOW_BASE_WEIGHT) * scale
            tooltip = f"{count} rutas (rumbo {avg_bearing_val:.0f}°)"
            if cluster["spread"] > 0:
                tooltip += f" (±{cluster['spread']:.0f}°)"
            popup_html = f"{style['name']}<br>{count} rutas agrupadas<br>Rumbo medio: {avg_bearing_val:.0f}°"
            flow_line = folium.PolyLine(
                [[avg_entry_lat, avg_entry_lon], [end_lat, end_lon]],
                color=style["color"],
                weight=weight,
                opacity=0.9,
                tooltip=tooltip,
                popup=folium.Popup(popup_html, max_width=260),
            )
            flow_line.add_to(flow_layer)
            mid_lat = (avg_entry_lat + end_lat) / 2.0
            mid_lon = (avg_entry_lon + end_lon) / 2.0
            folium.CircleMarker(
                [mid_lat, mid_lon],
                radius=max(6.0, weight * 0.6),
                color=style["color"],
                weight=0,
                opacity=0.0,
                fill=True,
                fill_color=style["color"],
                fill_opacity=0.08,
                tooltip=tooltip,
                popup=folium.Popup(popup_html, max_width=260),
            ).add_to(flow_layer)
            layer_has_data = True
        if layer_has_data:
            flow_bring_template = Template("""
            {% macro script(this, kwargs) %}
            var layer = {{this._parent.get_name()}};
            if (layer && layer.bringToFront){
                layer.bringToFront();
                layer.on('add', function(){
                    layer.bringToFront();
                });
            }
            {% endmacro %}
            """)
            flow_macro = MacroElement()
            flow_macro._template = flow_bring_template
            flow_layer.add_child(flow_macro)
            flow_layer.add_to(m)

    if sector_layer is not None:
        bring_template = Template("""
        {% macro script(this, kwargs) %}
        var layer = {{this._parent.get_name()}};
        if (layer && layer.bringToFront){
            layer.bringToFront();
            layer.on('add', function(){
                layer.bringToFront();
            });
        }
        {% endmacro %}
        """)
        bring_macro = MacroElement()
        bring_macro._template = bring_template
        sector_layer.add_child(bring_macro)
        sector_layer.add_to(m)

    add_region_panel(m, panel_series.to_dict(), {label: panel_colors[label] for label in panel_series.index})

    folium.LayerControl(collapsed=True).add_to(m)

    out_html = str(Path(args.out).resolve())
    m.save(out_html)

    # Resumen
    summary = (
        pd.DataFrame({"ORIGEN": [r["origin"] for r in routes], "DESTINO":[r["dest"] for r in routes]})
        .value_counts().reset_index(name="FRECUENCIA")
        .sort_values("FRECUENCIA", ascending=False)
    )
    csv_path = str(Path(out_html).with_suffix(".csv"))
    summary.to_csv(csv_path, index=False)

    print("HTML:", out_html)
    print("CSV :", csv_path)
    print("Columnas detectadas:", origin_col, "→", dest_col)
    print("Parejas trazadas:", len(routes))
    official_segments = sum(1 for r in routes if r.get("segment_method") == "sector")
    fallback_segments = len(routes) - official_segments
    synthetic_segments = sum(1 for r in routes if r.get("segment_method") != "sector" and r.get("sector_path"))
    if official_segments:
        print(f"Cruces con geometría oficial LECB E: {official_segments}")
    if fallback_segments:
        print(f"Cruces por aproximación geométrica: {fallback_segments}")
    if synthetic_segments:
        print(f"  ↳ de ellos con trayecto sintético dentro del sector: {synthetic_segments}")
    if unresolved:
        print("Sin resolver:", len(unresolved))
    if resolution_errors:
        items = list(resolution_errors.items())
        sample = items[:10]
        print("Detalle de resolución (muestra):")
        for code, reason in sample:
            print(f"  - {code}: {reason}")
        remaining = len(items) - len(sample)
        if remaining > 0:
            print(f"  ... {remaining} códigos adicionales")

    if opensky_auth and track_stats["attempted"]:
        print(f"Trayectorias OpenSky obtenidas: {track_stats['success']} de {track_stats['attempted']} (cache: {track_stats['cached']})")
    elif opensky_auth:
        print("Trayectorias OpenSky: sin resultados para los vuelos analizados.")

    generate_insights(df_original, departures_df, panel_series.to_dict(), args, region_label)

if __name__ == "__main__":
    main()
