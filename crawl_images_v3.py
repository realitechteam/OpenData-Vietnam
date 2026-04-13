#!/usr/bin/env python3
"""
Crawl images for OpenData-Vietnam POI.
Sources (all free):
  1. Wikipedia title search (vi + en) — best for named landmarks
  2. Wikimedia Commons search — best for well-known places with photos
Falls back gracefully. Stores image_url in _processed.json files.
"""
from __future__ import annotations
import json, os, time, urllib.parse, urllib.request
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed

SRC = '/tmp/OpenData-Vietnam'
HEADERS = {'User-Agent': 'OpenDataVN/1.0 (POI images; dev@realitechteam.com)'}

def api_get(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except:
        return {}

def norm(name):
    for p in ['Chùa ','Đền ','Đình ','Miếu ','Nhà thờ ','Bệnh viện ',
              'Khách sạn ','Hotel ','Nhà hàng ','Quán ','Công viên ',
              'Trung tâm ','Siêu thị ','Trạm ','Cửa hàng ','Phòng khám ',
              'Bãi biển ','Núi ','Sông ','Hồ ','Cầu ','Chợ ','Bảo tàng ',
              'Nhà hát ','Thác ','Vịnh ','Resort ','Spa ']:
        if name.startswith(p): return name[len(p):].lower().strip()
    return name.lower().strip()

def sim(a, b):
    na, nb = norm(a), norm(b)
    if na == nb: return 1.0
    if len(na) > 2 and len(nb) > 2 and (na in nb or nb in na): return 0.85
    return SequenceMatcher(None, na, nb).ratio()

# ── Source 1: Wikipedia title search ──
def wiki_search(name, lang='vi'):
    api = f'https://{lang}.wikipedia.org/w/api.php'
    p = urllib.parse.urlencode({
        'action':'query','generator':'search','gsrsearch': name,
        'gsrlimit':5,'prop':'pageimages','pithumbsize':800,'format':'json'
    })
    data = api_get(f'{api}?{p}')
    pages = data.get('query',{}).get('pages',{})
    best_img, best_s = None, 0
    for page in pages.values():
        src = page.get('thumbnail',{}).get('source')
        if not src: continue
        s = sim(name, page.get('title',''))
        if s > best_s: best_s = s; best_img = src
    return best_img if best_s >= 0.45 else None

# ── Source 2: Wikimedia Commons search ──
def commons_search(name):
    p = urllib.parse.urlencode({
        'action':'query','generator':'search',
        'gsrsearch': f'{name}',
        'gsrnamespace': 6,  # File namespace
        'gsrlimit': 3,
        'prop': 'imageinfo',
        'iiprop': 'url',
        'iiurlwidth': 800,
        'format': 'json'
    })
    data = api_get(f'https://commons.wikimedia.org/w/api.php?{p}')
    pages = data.get('query',{}).get('pages',{})
    for page in pages.values():
        title = page.get('title','').lower()
        # Skip PDFs, SVGs, non-photo files
        if any(title.endswith(ext) for ext in ['.pdf','.svg','.ogg','.ogv','.webm']):
            continue
        info = page.get('imageinfo',[{}])
        if info:
            thumb = info[0].get('thumburl','')
            if thumb and 'upload.wikimedia.org' in thumb:
                return thumb
    return None

def find_image(name):
    """Try all sources in order."""
    # 1) Vietnamese Wikipedia
    img = wiki_search(name, 'vi')
    if img: return img

    # 2) English Wikipedia
    img = wiki_search(name, 'en')
    if img: return img

    # 3) Wikimedia Commons
    img = commons_search(name)
    if img: return img

    return None

def process_file(filepath, max_records=0):
    with open(filepath) as f:
        data = json.load(f)

    targets = [d for d in data
               if 8.18 <= d.get('lat',0) <= 23.39
               and 102.14 <= d.get('lon',0) <= 109.46
               and d.get('name','').strip()
               and not d.get('image_url')]

    # Deduplicate by name (same name = same image)
    seen_names = {}
    unique_targets = []
    for d in targets:
        n = d['name'].strip()
        if n not in seen_names:
            seen_names[n] = []
            unique_targets.append(d)
        seen_names[n].append(d)

    if max_records > 0:
        unique_targets = unique_targets[:max_records]

    fname = os.path.basename(filepath)
    total = len(unique_targets)
    found = 0

    print(f"\n{'='*50}", flush=True)
    print(f"{fname}: {total} unique names ({len(targets)} records)", flush=True)
    print(f"{'='*50}", flush=True)

    by_id = {d.get('osm_id'): d for d in data if d.get('osm_id')}

    def set_image(name, img):
        """Set image for all records with this name."""
        for rec in seen_names.get(name, []):
            oid = rec.get('osm_id')
            if oid and oid in by_id:
                by_id[oid]['image_url'] = img
            else:
                for d in data:
                    if d.get('lat') == rec['lat'] and d.get('lon') == rec['lon'] and d.get('name') == rec['name']:
                        d['image_url'] = img
                        break

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {}
        for rec in unique_targets:
            f = pool.submit(find_image, rec['name'])
            futures[f] = rec

        for i, f in enumerate(as_completed(futures)):
            rec = futures[f]
            try:
                img = f.result()
                if img:
                    found += 1
                    set_image(rec['name'], img)
            except:
                pass

            if (i+1) % 100 == 0 or i+1 == total:
                pct = found/(i+1)*100
                print(f"  [{i+1}/{total}] images: {found} ({pct:.0f}%)", flush=True)

    with open(filepath, 'w') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',',':'))

    rate = found/total*100 if total > 0 else 0
    print(f"  DONE: {found}/{total} unique names ({rate:.1f}%)", flush=True)
    return total, found

def main():
    configs = [
        ('POI/tourism_processed.json', 0),
        ('POI/places_of_worship_processed.json', 2000),
        ('Natural/natural_features_processed.json', 1500),
        ('POI/hospitals_processed.json', 0),
        ('POI/accommodation_processed.json', 2000),
        ('POI/food_dining_processed.json', 3000),
        ('POI/markets_processed.json', 1500),
        ('POI/healthcare_extended_processed.json', 800),
        ('POI/fuel_stations_processed.json', 500),
    ]

    gt, gf = 0, 0
    for fpath, maxr in configs:
        full = os.path.join(SRC, fpath)
        if not os.path.exists(full):
            print(f"SKIP: {fpath}", flush=True)
            continue
        t, f = process_file(full, maxr)
        gt += t; gf += f

    print(f"\n{'='*50}", flush=True)
    print(f"GRAND TOTAL: {gf}/{gt} images ({gf/gt*100:.1f}%)", flush=True)

if __name__ == '__main__':
    main()
