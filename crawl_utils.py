#!/usr/bin/env python3
"""Utility to crawl and process OSM data for Vietnam Open Data project."""
import json
import os
import time
import urllib.request
import urllib.parse
import sys

BASE = '/Users/bami/Desktop/04_OpenData_Vietnam'
OVERPASS_URL = 'https://overpass-api.de/api/interpreter'
BBOX_VN = '8.0,102.0,23.5,110.0'

def overpass_query(query, max_retries=3, timeout=120):
    """Execute Overpass API query with retry logic."""
    for attempt in range(max_retries):
        try:
            data = urllib.parse.urlencode({'data': query}).encode()
            req = urllib.request.Request(OVERPASS_URL, data=data)
            req.add_header('User-Agent', 'OpenData-Vietnam-Crawler/1.0')
            resp = urllib.request.urlopen(req, timeout=timeout)
            return json.loads(resp.read())
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                wait = 15 * (attempt + 1)
                print(f"  Waiting {wait}s before retry...")
                time.sleep(wait)
    return None

def process_elements(data):
    """Process OSM elements into clean POI format."""
    if not data or 'elements' not in data:
        return []

    # Build node lookup for way center calculation
    all_nodes = {}
    for e in data['elements']:
        if e['type'] == 'node':
            all_nodes[e['id']] = (e.get('lat'), e.get('lon'))

    pois = []
    seen = set()
    for e in data['elements']:
        if 'tags' not in e or e['id'] in seen:
            continue
        seen.add(e['id'])
        tags = e.get('tags', {})

        if e['type'] == 'node':
            lat, lon = e.get('lat'), e.get('lon')
        elif e['type'] == 'way':
            nids = e.get('nodes', [])
            coords = [(all_nodes[n][0], all_nodes[n][1])
                      for n in nids if n in all_nodes and all_nodes[n][0]]
            if not coords:
                continue
            lat = sum(c[0] for c in coords) / len(coords)
            lon = sum(c[1] for c in coords) / len(coords)
        elif 'center' in e:
            lat = e['center'].get('lat')
            lon = e['center'].get('lon')
        else:
            continue

        if not lat or not lon:
            continue

        # Determine category from tags
        subcat = ''
        for key in ['amenity', 'tourism', 'historic', 'shop', 'leisure',
                     'office', 'healthcare', 'man_made', 'natural',
                     'railway', 'aeroway', 'highway', 'building', 'religion']:
            if key in tags:
                subcat = tags[key]
                break

        pois.append({
            'osm_id': e['id'],
            'osm_type': e['type'],
            'subcategory': subcat,
            'lat': lat,
            'lon': lon,
            'name': tags.get('name', ''),
            'name_en': tags.get('name:en', ''),
            'name_vi': tags.get('name:vi', ''),
            'operator': tags.get('operator', ''),
            'brand': tags.get('brand', ''),
            'phone': tags.get('phone', ''),
            'website': tags.get('website', ''),
            'opening_hours': tags.get('opening_hours', ''),
            'address': tags.get('addr:street', ''),
            'housenumber': tags.get('addr:housenumber', ''),
            'city': tags.get('addr:city', ''),
            'district': tags.get('addr:district', ''),
            'province': tags.get('addr:province', ''),
        })
    return pois

def crawl_and_save(name, queries, output_dir, output_file, category, delay=5):
    """Crawl OSM data and save processed result."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_file)

    all_pois = []
    seen_ids = set()

    for i, query in enumerate(queries):
        print(f"  Query {i+1}/{len(queries)}...")
        data = overpass_query(query)
        if data:
            pois = process_elements(data)
            for p in pois:
                if p['osm_id'] not in seen_ids:
                    p['category'] = category
                    all_pois.append(p)
                    seen_ids.add(p['osm_id'])
            print(f"    Got {len(pois)} items (total unique: {len(all_pois)})")
        else:
            print(f"    FAILED")

        if i < len(queries) - 1:
            time.sleep(delay)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_pois, f, ensure_ascii=False, indent=2)

    print(f"  Saved {len(all_pois)} records to {output_file}")
    return len(all_pois)
