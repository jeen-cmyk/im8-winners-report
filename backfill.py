#!/usr/bin/env python3
"""
One-time backfill script.
Pulls 4 x L7D windows going back 28 days and seeds run_history.json.
"""

import os, json, re, sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError

ACCESS_TOKEN  = os.environ.get("META_ACCESS_TOKEN", "")
AD_ACCOUNT_ID = "act_1000723654649396"
API_VERSION   = "v20.0"
BASE          = f"https://graph.facebook.com/{API_VERSION}"
HISTORY_FILE  = "run_history.json"
MAX_RUNS      = 8

ROAS_THRESHOLD  = 1.0
PURCH_THRESHOLD = 10

WINNER_POOL_KW = ['Winner','Winners','WINNER','TOP30','l7d winner','TOP 50']
ICP_KW         = ['ICP','GLP','Menopause','Collagen','ANGLE','ACTIVE SENIOR','Senior',
                  'Cognitive','Immune','Fitness','Sleep','Weight','Gut','Joint','Pill',
                  'Energy','Green','Young Prof','Persona','NERMW','HCSS','RECOVERY',
                  'Aging Athlete','Performance','FREQUENTFLYER','Traveler','Travel']
L3_EXCL_KW     = ['Retargeting','ENGAGER','ATC','GEISTM']

LP_MAP = {
    'HOMEPAGE':'https://im8health.com/','PDP':'https://im8health.com/products/essentials',
    'GETPDP':'https://get.im8health.com/essentials',
    'GLP1LDP':'https://get.im8health.com/pages/glp1',
    'FEELAGAINLDP':'https://get.im8health.com/pages/feel-again',
    'BKMFORMULALDP':'https://get.im8health.com/pages/beckham-formula',
    'NOBSLDP':'https://get.im8health.com/pages/no-bs',
    'WHYIM8LDP':'https://get.im8health.com/pages/why-im8',
    'SENIORSLDP':'https://get.im8health.com/pages/seniors',
    'MENOPAUSELDP':'https://get.im8health.com/pages/menopause',
    '16IN1DRJAMES':'https://get.im8health.com/pages/dr-james',
    'GETGUTLDP':'https://get.im8health.com/for/gut',
    'GETRECOVERYACTLDP':'https://get.im8health.com/recovery/active',
    'GETJOINTSLDP':'https://get.im8health.com/for/joints',
    'GETTRAVELLDP':'https://get.im8health.com/for/travel',
    'SCIENCELDP':'https://get.im8health.com/pages/science',
    'PROOFLDP':'https://get.im8health.com/pages/proof',
    'ACTNOWLDP':'https://get.im8health.com/pages/act-now',
    'PROUPGRADELDP':'https://get.im8health.com/pages/pro-upgrade',
    'V2UPGRADELDP':'https://im8health.com/pages/essentials-pro-release',
    'PROMPTLDP':'https://get.im8health.com/prompt',
}

def api_get(path, params):
    params['access_token'] = ACCESS_TOKEN
    url = f"{BASE}/{path}?{urlencode(params)}"
    try:
        req = Request(url, headers={'User-Agent': 'IM8WinnersBot/1.0'})
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  API error: {e}"); return None

def paginate(path, params):
    results = []
    data = api_get(path, params)
    if not data: return results
    results.extend(data.get('data', []))
    while 'paging' in data and 'next' in data['paging']:
        try:
            req = Request(data['paging']['next'], headers={'User-Agent': 'IM8WinnersBot/1.0'})
            with urlopen(req, timeout=30) as r:
                data = json.loads(r.read())
            results.extend(data.get('data', []))
        except: break
    return results

def get_tier(c):
    s = str(c)
    for t in ['XX','L1','L2','L3']:
        if s.startswith(t): return t
    return 'OTHER'

def is_winner_pool(s): return any(k in str(s) for k in WINNER_POOL_KW)
def is_icp(s):         return any(k in str(s) for k in ICP_KW)
def is_excl_l3(c):     return any(k in str(c) for k in L3_EXCL_KW)

def ad_type(name):
    n = str(name).upper()
    if 'KOLUGC' in n or 'KOL_UGC' in n: return 'KOL UGC'
    if 'CREATORUGC' in n: return 'Creator UGC'
    if 'JAMESPOST' in n or 'IGPOST' in n: return 'IG Post'
    if any(x in n for x in ['_VID_','_VSL_','_WOTXT_','_TALKH_','_VLOG_']) or n.startswith('VID_'): return 'Video'
    if '_IMG_' in n or n.startswith('IMG_'): return 'Static'
    return 'Other'

def get_note(ad_name, adset_name):
    if is_icp(adset_name): return 'ICP/Persona — tag only'
    n = str(ad_name).upper()
    if 'KOLUGC' in n or 'KOL_UGC' in n: return 'KOL UGC — dupe to pool'
    if 'CREATORUGC' in n: return 'Creator UGC — dupe to pool'
    return 'Generic — dupe to pool'

def note_type(note):
    if 'ICP' in note: return 'icp'
    if 'KOL' in note or 'Creator' in note: return 'kol'
    return 'generic'

def get_lp(name):
    for tok in reversed([t.strip().strip('*') for t in str(name).split('_')]):
        if tok in LP_MAP: return LP_MAP[tok]
    return ''

def win_tag_for(dt):
    return f"WIN{dt.strftime('%y')}{dt.strftime('%b').upper()[:2]}W{(dt.day-1)//7+1}"

def run_label_for(dt):
    # Approximate: even weeks = Monday, odd = Friday
    return ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][dt.weekday()]

def fetch_window(date_end):
    date_start = date_end - timedelta(days=7)
    ds = date_start.strftime('%Y-%m-%d')
    de = date_end.strftime('%Y-%m-%d')
    print(f"\n--- Pulling {ds} → {de} ---")
    fields = 'ad_name,ad_id,campaign_name,adset_name,spend,purchase_roas,actions,action_values,ctr'
    params = {'level':'ad','fields':fields,
              'time_range':json.dumps({'since':ds,'until':de}),'limit':500}
    raw = paginate(f"{AD_ACCOUNT_ID}/insights", params)
    print(f"  {len(raw)} rows")
    return raw, ds, de

def parse_ad(ad):
    def ga(actions, t):
        return next((float(a['value']) for a in (actions or []) if a.get('action_type')==t), 0)
    purchases = ga(ad.get('actions',[]), 'purchase')
    revenue   = ga(ad.get('action_values',[]), 'purchase')
    spend     = float(ad.get('spend', 0))
    roas_raw  = ad.get('purchase_roas', [])
    roas      = float(roas_raw[0]['value']) if roas_raw else (revenue/spend if spend>0 else 0)
    return {
        'ad_id':         ad.get('ad_id',''),
        'ad_name':       ad.get('ad_name',''),
        'campaign_name': ad.get('campaign_name',''),
        'adset_name':    ad.get('adset_name',''),
        'spend':    round(spend,2), 'roas': round(roas,2),
        'purchases': int(purchases), 'revenue': round(revenue,2),
        'cpa':      round(spend/purchases,2) if purchases>0 else 0,
        'ctr':      round(float(ad.get('ctr',0)),2),
        'thumbnail': '', 'fb_link': '',
    }

def classify_ads(ads):
    untagged, tagged = [], []
    for ad in ads:
        name  = ad['ad_name']
        camp  = ad['campaign_name']
        adset = ad['adset_name']
        tier  = get_tier(camp)
        is_tagged = bool(re.search(r'WIN2\d', str(name)))
        if tier not in ('L1','L2','L3'): continue
        if tier == 'L3' and is_excl_l3(camp): continue
        if tier == 'L1' and is_winner_pool(adset): continue
        if ad['roas'] <= ROAS_THRESHOLD: continue
        if ad['purchases'] <= PURCH_THRESHOLD: continue
        if ad['spend'] <= 0: continue
        note = get_note(name, adset)
        ad.update({'tier': tier, 'ad_type': ad_type(name),
                   'note': note, 'note_type': note_type(note),
                   'lp': get_lp(name), 'tagged': is_tagged})
        if is_tagged: tagged.append(ad)
        else: untagged.append(ad)
    untagged.sort(key=lambda x: (0 if x['note_type']=='icp' else 1, -x['roas']))
    tagged.sort(key=lambda x: -x['roas'])
    return untagged, tagged

def fetch_creatives(ad_ids):
    if not ad_ids: return {}
    creatives = {}
    for i in range(0, len(ad_ids), 50):
        batch = ad_ids[i:i+50]
        data = api_get(f"{AD_ACCOUNT_ID}/ads", {
            'fields': 'id,creative{thumbnail_url,effective_object_story_id}',
            'filtering': json.dumps([{'field':'id','operator':'IN','value':batch}]),
            'limit': 50,
        })
        if not data: continue
        for ad in data.get('data', []):
            aid = ad.get('id','')
            cr  = ad.get('creative', {})
            thumb = cr.get('thumbnail_url','')
            story_id = cr.get('effective_object_story_id','')
            fb_link = ''
            if story_id:
                parts = story_id.split('_', 1)
                if len(parts) == 2:
                    fb_link = f"https://www.facebook.com/permalink.php?story_fbid={parts[1]}&id={parts[0]}"
            creatives[aid] = {'thumbnail': thumb, 'fb_link': fb_link}
    print(f"  Creatives: {len(creatives)}")
    return creatives

if __name__ == '__main__':
    if not ACCESS_TOKEN:
        print("ERROR: META_ACCESS_TOKEN not set"); sys.exit(1)

    now = datetime.now()

    # 4 windows: today, -7d, -14d, -21d
    # Label them as Mon/Fri alternating going backwards
    windows = [
        (now,           'Friday'),
        (now - timedelta(days=7),  'Monday'),
        (now - timedelta(days=14), 'Friday'),
        (now - timedelta(days=21), 'Monday'),
    ]

    runs = []
    for (end_dt, run_day) in windows:
        raw, ds, de = fetch_window(end_dt)
        ads = [parse_ad(a) for a in raw]
        untagged, tagged = classify_ads(ads)
        all_ads = untagged + tagged

        # Fetch creatives
        all_ids = list(set(a['ad_id'] for a in all_ads if a.get('ad_id')))
        creatives = fetch_creatives(all_ids)
        for a in all_ads:
            cr = creatives.get(a.get('ad_id',''), {})
            a['thumbnail'] = cr.get('thumbnail','')
            a['fb_link']   = cr.get('fb_link','')

        d_obj = datetime.strptime(de, '%Y-%m-%d')
        run_entry = {
            'tag':        win_tag_for(end_dt),
            'run_day':    run_day,
            'run_date':   end_dt.strftime('%Y-%m-%d'),
            'tab_date':   end_dt.strftime('%-d %b'),
            'date_range': f"{datetime.strptime(ds,'%Y-%m-%d').strftime('%-d %b')} – {d_obj.strftime('%-d %b %Y')}",
            'ads':        all_ads,
        }
        runs.append(run_entry)
        print(f"  → {len(untagged)} untagged + {len(tagged)} tagged = {len(all_ads)} total")

    history = {'runs': runs[:MAX_RUNS]}
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)
    print(f"\n✅ Backfill done — {len(runs)} runs saved to {HISTORY_FILE}")
