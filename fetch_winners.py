#!/usr/bin/env python3
"""
IM8 Winners Report — Meta API
Runs Mon + Fri. Pulls L7D winners, fetches creatives, builds history dashboard.
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

# ── API ───────────────────────────────────────────────────────────────────────
def api_get(path, params):
    params['access_token'] = ACCESS_TOKEN
    url = f"{BASE}/{path}?{urlencode(params)}"
    try:
        req = Request(url, headers={'User-Agent': 'IM8WinnersBot/1.0'})
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  API error [{path}]: {e}"); return None

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

# ── HELPERS ───────────────────────────────────────────────────────────────────
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

def win_tag():
    d = datetime.now()
    return f"WIN{d.strftime('%y')}{d.strftime('%b').upper()[:2]}W{(d.day-1)//7+1}"

def run_label():
    return 'Monday' if datetime.now().weekday() == 0 else 'Friday'

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

def get_lp(name):
    for tok in reversed([t.strip().strip('*') for t in str(name).split('_')]):
        if tok in LP_MAP: return LP_MAP[tok]
    return ''

# ── FETCH ADS ─────────────────────────────────────────────────────────────────
def fetch_ads():
    end   = datetime.now()
    start = end - timedelta(days=7)
    ds, de = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
    print(f"Fetching insights {ds} → {de}...")
    fields = 'ad_name,ad_id,campaign_name,adset_name,spend,purchase_roas,actions,action_values,ctr'
    params = {'level':'ad','fields':fields,
              'time_range':json.dumps({'since':ds,'until':de}),'limit':500}
    ads = paginate(f"{AD_ACCOUNT_ID}/insights", params)
    print(f"  → {len(ads)} rows")
    return ads, ds, de

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

# ── FETCH CREATIVES ───────────────────────────────────────────────────────────
def fetch_creatives(ad_ids):
    """Fetch thumbnail + FB post link for each ad_id."""
    if not ad_ids: return {}
    creatives = {}
    # batch in groups of 50
    for i in range(0, len(ad_ids), 50):
        batch = ad_ids[i:i+50]
        ids_str = ','.join(batch)
        data = api_get(f"{AD_ACCOUNT_ID}/ads", {
            'fields': 'id,name,creative{thumbnail_url,effective_object_story_id}',
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
                # story_id format: page_id_post_id
                parts = story_id.split('_', 1)
                if len(parts) == 2:
                    fb_link = f"https://www.facebook.com/permalink.php?story_fbid={parts[1]}&id={parts[0]}"
            creatives[aid] = {'thumbnail': thumb, 'fb_link': fb_link}
    print(f"  → Creatives fetched for {len(creatives)} ads")
    return creatives

# ── WINNER LOGIC ──────────────────────────────────────────────────────────────
def classify_ads(ads):
    untagged, tagged = [], []
    already_tagged_count = 0
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
        ad.update({
            'tier': tier, 'ad_type': ad_type(name),
            'note': note, 'note_type': note_type(note),
            'lp': get_lp(name), 'tagged': is_tagged,
        })

        if is_tagged:
            already_tagged_count += 1
            tagged.append(ad)
        else:
            untagged.append(ad)

    untagged.sort(key=lambda x: (0 if x['note_type']=='icp' else 1, -x['roas']))
    tagged.sort(key=lambda x: -x['roas'])
    return untagged, tagged

# ── HISTORY ───────────────────────────────────────────────────────────────────
def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f: return json.load(f)
        except: pass
    return {'runs': []}

def save_history(history):
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)

# ── HTML ──────────────────────────────────────────────────────────────────────
def generate_html(history):
    runs_json = json.dumps(history['runs'])
    now_str   = datetime.now().strftime('%-d %b %Y, %H:%M UTC')

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>IM8 Winners Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0a0a0f;--surface:#111118;--surface2:#18181f;--border:rgba(255,255,255,.07);--gold:#e8b450;--gold-dim:rgba(232,180,80,.12);--teal:#3ecfb2;--teal-dim:rgba(62,207,178,.1);--purple:#9b7cff;--purple-dim:rgba(155,124,255,.1);--blue:#60a5fa;--green:#4ade80;--amber:#fb923c;--red:#f87171;--white:#f0f0f8;--muted:#5a5a7a;--l1:#fb923c;--l3:#60a5fa;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:var(--bg);color:var(--white);font-family:"DM Sans",sans-serif;font-weight:300;min-height:100vh;}}
body::before{{content:"";position:fixed;inset:0;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='.04'/%3E%3C/svg%3E");pointer-events:none;z-index:0;opacity:.4;}}

/* Layout */
.app{{position:relative;z-index:1;display:flex;flex-direction:column;min-height:100vh;}}
.topbar{{background:var(--surface);border-bottom:1px solid var(--border);padding:0 32px;display:flex;align-items:stretch;justify-content:space-between;gap:0;position:sticky;top:0;z-index:100;}}
.brand{{display:flex;align-items:center;gap:12px;padding:16px 0;border-right:1px solid var(--border);padding-right:24px;margin-right:8px;}}
.brand-dot{{width:8px;height:8px;background:var(--gold);border-radius:50%;animation:pulse 2s infinite;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.brand-name{{font-family:"Syne",sans-serif;font-size:15px;font-weight:700;color:var(--gold);letter-spacing:-.01em;}}
.tabs{{display:flex;align-items:stretch;gap:0;overflow-x:auto;flex:1;}}
.tabs::-webkit-scrollbar{{display:none;}}
.tab{{display:flex;flex-direction:column;justify-content:center;padding:0 20px;cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap;min-width:fit-content;border-right:1px solid var(--border);}}
.tab:hover{{background:rgba(255,255,255,.03);}}
.tab.active{{border-bottom-color:var(--gold);background:rgba(232,180,80,.05);}}
.tab-day{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px;}}
.tab.active .tab-day{{color:var(--gold);}}
.tab-date{{font-size:13px;font-weight:500;color:var(--white);}}
.tab-count{{font-family:"DM Mono",monospace;font-size:10px;margin-top:2px;}}
.tab.active .tab-count{{color:var(--green);}}
.tab-count.has-untagged{{color:var(--amber);}}
.topbar-right{{display:flex;align-items:center;gap:12px;padding:0 0 0 16px;border-left:1px solid var(--border);margin-left:8px;}}
.live-badge{{display:flex;align-items:center;gap:6px;font-family:"DM Mono",monospace;font-size:10px;color:var(--green);}}
.live-dot{{width:6px;height:6px;background:var(--green);border-radius:50%;animation:pulse 2s infinite;}}
.last-updated{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);}}

/* Main */
.main{{flex:1;padding:32px;max-width:1400px;margin:0 auto;width:100%;}}
.run-header{{margin-bottom:28px;}}
.run-title{{font-family:"Syne",sans-serif;font-size:clamp(24px,3vw,40px);font-weight:800;letter-spacing:-.03em;}}
.run-title span{{color:var(--gold);}}
.run-meta{{font-family:"DM Mono",monospace;font-size:12px;color:var(--muted);margin-top:6px;display:flex;gap:16px;flex-wrap:wrap;}}
.run-meta b{{color:var(--white);}}

/* Stats row */
.stats-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:32px;}}
.stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;position:relative;overflow:hidden;}}
.stat::after{{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--gold),transparent);opacity:.4;}}
.stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:5px;font-family:"DM Mono",monospace;}}
.stat-value{{font-family:"Syne",sans-serif;font-size:22px;font-weight:700;}}

/* Section */
.section{{margin-bottom:32px;}}
.section-head{{display:flex;align-items:center;gap:12px;margin-bottom:14px;}}
.section-head h2{{font-family:"Syne",sans-serif;font-size:16px;font-weight:700;}}
.section-badge{{font-family:"DM Mono",monospace;font-size:11px;padding:3px 10px;border-radius:20px;}}
.section-badge.action{{background:rgba(251,146,60,.15);color:var(--amber);border:1px solid rgba(251,146,60,.3);}}
.section-badge.done{{background:rgba(74,222,128,.1);color:var(--green);border:1px solid rgba(74,222,128,.2);}}
.section-line{{flex:1;height:1px;background:var(--border);}}
.toggle-tagged{{font-family:"DM Mono",monospace;font-size:11px;color:var(--muted);cursor:pointer;padding:3px 10px;border:1px solid var(--border);border-radius:20px;background:transparent;transition:all .2s;}}
.toggle-tagged:hover{{border-color:var(--green);color:var(--green);}}

/* Cards grid */
.cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:12px;}}

/* Card */
.card{{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;transition:border-color .2s,transform .2s;animation:fadeUp .3s ease both;}}
.card:hover{{border-color:rgba(232,180,80,.25);transform:translateY(-2px);}}
.card.tagged-card{{opacity:.6;}}
.card.tagged-card:hover{{opacity:1;}}

/* Card thumbnail */
.card-thumb{{position:relative;width:100%;aspect-ratio:16/9;background:var(--surface2);overflow:hidden;}}
.card-thumb img{{width:100%;height:100%;object-fit:cover;display:block;}}
.card-thumb .no-thumb{{display:flex;align-items:center;justify-content:center;width:100%;height:100%;color:var(--muted);font-family:"DM Mono",monospace;font-size:11px;}}
.thumb-badges{{position:absolute;top:8px;left:8px;display:flex;gap:6px;flex-wrap:wrap;}}
.thumb-type{{font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:3px 8px;border-radius:4px;backdrop-filter:blur(8px);}}
.thumb-type.kol{{background:rgba(62,207,178,.8);color:#000;}}
.thumb-type.generic{{background:rgba(232,180,80,.8);color:#000;}}
.thumb-type.icp{{background:rgba(155,124,255,.8);color:#fff;}}
.thumb-type.static{{background:rgba(96,165,250,.8);color:#000;}}
.thumb-tagged{{position:absolute;top:8px;right:8px;background:rgba(74,222,128,.9);color:#000;font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:3px 8px;border-radius:4px;}}
.fb-link{{position:absolute;bottom:8px;right:8px;background:rgba(10,10,15,.8);border:1px solid rgba(255,255,255,.15);border-radius:6px;padding:5px 10px;font-family:"DM Mono",monospace;font-size:10px;color:var(--blue);text-decoration:none;backdrop-filter:blur(8px);transition:all .2s;display:flex;align-items:center;gap:5px;}}
.fb-link:hover{{background:rgba(96,165,250,.2);border-color:var(--blue);}}

/* Card body */
.card-body{{padding:14px 16px;}}
.card-adname{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:6px;}}
.card-metrics{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;}}
.metric{{display:flex;flex-direction:column;gap:2px;}}
.metric-label{{font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-family:"DM Mono",monospace;}}
.metric-value{{font-family:"Syne",sans-serif;font-size:16px;font-weight:700;line-height:1;}}
.metric-value.great{{color:var(--green);}} .metric-value.good{{color:#a3e635;}} .metric-value.ok{{color:var(--amber);}}
.card-footer{{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;}}
.badges{{display:flex;gap:6px;flex-wrap:wrap;}}
.badge{{font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:3px 8px;border-radius:4px;}}
.badge.tier-L1{{background:rgba(251,146,60,.15);color:var(--l1);border:1px solid rgba(251,146,60,.3);}}
.badge.tier-L3{{background:rgba(96,165,250,.1);color:var(--l3);border:1px solid rgba(96,165,250,.25);}}
.badge.kol{{background:var(--teal-dim);color:var(--teal);border:1px solid rgba(62,207,178,.2);}}
.badge.generic{{background:var(--gold-dim);color:var(--gold);}}
.badge.icp{{background:var(--purple-dim);color:var(--purple);border:1px solid rgba(155,124,255,.2);}}
.lp-link{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);text-decoration:none;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.lp-link:hover{{color:var(--gold);}}

/* Empty */
.empty-run{{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:80px 40px;text-align:center;}}
.empty-run .emoji{{font-size:48px;margin-bottom:16px;}}
.empty-run h3{{font-family:"Syne",sans-serif;font-size:22px;font-weight:700;margin-bottom:8px;}}
.empty-run p{{font-family:"DM Mono",monospace;font-size:12px;color:var(--muted);}}
.no-data{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:32px;text-align:center;font-family:"DM Mono",monospace;font-size:12px;color:var(--muted);}}

@keyframes fadeUp{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:translateY(0)}}}}
@media(max-width:768px){{.stats-row{{grid-template-columns:repeat(3,1fr);}} .cards-grid{{grid-template-columns:1fr;}} .main{{padding:16px;}}}}
</style>
</head>
<body>
<div class="app">

  <!-- Top bar -->
  <div class="topbar">
    <div class="brand">
      <div class="brand-dot"></div>
      <div class="brand-name">IM8 Winners</div>
    </div>
    <div class="tabs" id="tabs"></div>
    <div class="topbar-right">
      <div class="live-badge"><div class="live-dot"></div>Live</div>
      <div class="last-updated" id="lastUpdated"></div>
    </div>
  </div>

  <!-- Main content -->
  <div class="main" id="main">
    <div class="empty-run">
      <div class="emoji">📊</div>
      <h3>No runs yet</h3>
      <p>Run the workflow to generate the first winners report</p>
    </div>
  </div>

</div>
<script>
const RUNS = {runs_json};

document.getElementById('lastUpdated').textContent = 'Updated: {now_str}';

function roasCls(r) {{ return r>=2?'great':r>=1.5?'good':'ok'; }}

function thumbHTML(ad) {{
  if (ad.thumbnail) {{
    return `<img src="${{ad.thumbnail}}" alt="" loading="lazy" onerror="this.parentElement.innerHTML='<div class=\\"no-thumb\\">No preview</div>'">`;
  }}
  return `<div class="no-thumb">No preview</div>`;
}}

function typeClass(nt) {{
  if (nt==='kol') return 'kol';
  if (nt==='icp') return 'icp';
  if (nt==='generic') return 'generic';
  return 'static';
}}

function typeLabel(nt, adType) {{
  if (nt==='kol') return adType||'KOL UGC';
  if (nt==='icp') return 'ICP';
  return adType||'Static';
}}

function buildCard(ad) {{
  const tc = typeClass(ad.note_type);
  const tl = typeLabel(ad.note_type, ad.ad_type);
  const fbBtn = ad.fb_link
    ? `<a class="fb-link" href="${{ad.fb_link}}" target="_blank">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>
        View Post
      </a>`
    : '';
  const taggedBadge = ad.tagged ? `<div class="thumb-tagged">✓ Tagged</div>` : '';
  const lp = ad.lp ? `<a class="lp-link" href="${{ad.lp}}" target="_blank" title="${{ad.lp}}">${{ad.lp.replace('https://','').replace('get.im8health.com','').replace('im8health.com','')||ad.lp}}</a>` : '<span style="font-family:\'DM Mono\',monospace;font-size:10px;color:var(--muted)">—</span>';

  return `<div class="card${{ad.tagged?' tagged-card':''}}">
    <div class="card-thumb">
      ${{thumbHTML(ad)}}
      <div class="thumb-badges">
        <span class="thumb-type ${{tc}}">${{tl}}</span>
      </div>
      ${{taggedBadge}}
      ${{fbBtn}}
    </div>
    <div class="card-body">
      <div class="card-adname" title="${{ad.ad_name}}">${{ad.ad_name}}</div>
      <div class="card-metrics">
        <div class="metric">
          <div class="metric-label">ROAS</div>
          <div class="metric-value ${{roasCls(ad.roas)}}">${{ad.roas.toFixed(2)}}x</div>
        </div>
        <div class="metric">
          <div class="metric-label">Purchases</div>
          <div class="metric-value">${{ad.purchases}}</div>
        </div>
        <div class="metric">
          <div class="metric-label">Spend</div>
          <div class="metric-value" style="font-size:14px;color:var(--amber)">${{ad.spend>0?'$'+ad.spend.toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}):' —'}}</div>
        </div>
        <div class="metric">
          <div class="metric-label">Revenue</div>
          <div class="metric-value" style="font-size:14px;color:var(--green)">${{ad.revenue>0?'$'+ad.revenue.toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}):' —'}}</div>
        </div>
      </div>
      <div class="card-footer">
        <div class="badges">
          <span class="badge tier-${{ad.tier}}">${{ad.tier}}</span>
          <span class="badge ${{ad.note_type}}">${{ad.note}}</span>
        </div>
        ${{lp}}
      </div>
    </div>
  </div>`;
}}

function renderRun(run) {{
  const untagged = run.ads.filter(a => !a.tagged);
  const tagged   = run.ads.filter(a => a.tagged);
  const avgRoas  = run.ads.length ? (run.ads.reduce((s,a)=>s+a.roas,0)/run.ads.length).toFixed(2) : '—';
  const totalRev = run.ads.reduce((s,a)=>s+a.revenue,0);
  const totalP   = run.ads.reduce((s,a)=>s+a.purchases,0);

  let html = `
    <div class="run-header">
      <div class="run-title">Winners &nbsp;<span>${{run.tag}}</span></div>
      <div class="run-meta">
        <span>📅 <b>${{run.date_range}}</b></span>
        <span>🗓 <b>${{run.run_day}} cut</b></span>
        <span>🔴 <b>${{untagged.length}}</b> needs action</span>
        <span>✅ <b>${{tagged.length}}</b> tagged</span>
      </div>
    </div>
    <div class="stats-row">
      <div class="stat"><div class="stat-label">Needs Action</div><div class="stat-value" style="color:var(--amber)">${{untagged.length}}</div></div>
      <div class="stat"><div class="stat-label">Already Tagged</div><div class="stat-value" style="color:var(--green)">${{tagged.length}}</div></div>
      <div class="stat"><div class="stat-label">Avg ROAS</div><div class="stat-value" style="color:var(--gold)">${{avgRoas}}x</div></div>
      <div class="stat"><div class="stat-label">Total Purchases</div><div class="stat-value">${{totalP}}</div></div>
      <div class="stat"><div class="stat-label">Revenue</div><div class="stat-value" style="color:var(--green);font-size:18px">${{totalRev>0?'$'+totalRev.toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}):' —'}}</div></div>
    </div>`;

  // Untagged section
  html += `<div class="section">
    <div class="section-head">
      <h2>🔴 Needs Action</h2>
      <span class="section-badge action">${{untagged.length}} ads</span>
      <div class="section-line"></div>
    </div>
    ${{untagged.length
      ? `<div class="cards-grid">${{untagged.map(buildCard).join('')}}</div>`
      : '<div class="no-data">No untagged winners this window ✓</div>'
    }}
  </div>`;

  // Tagged section (collapsed)
  html += `<div class="section">
    <div class="section-head">
      <h2>✅ Already Tagged</h2>
      <span class="section-badge done">${{tagged.length}} ads</span>
      <div class="section-line"></div>
      <button class="toggle-tagged" onclick="toggleTagged(this)">Show</button>
    </div>
    <div class="tagged-body" style="display:none">
      ${{tagged.length
        ? `<div class="cards-grid">${{tagged.map(buildCard).join('')}}</div>`
        : '<div class="no-data">None</div>'
      }}
    </div>
  </div>`;

  return html;
}}

function toggleTagged(btn) {{
  const body = btn.closest('.section').querySelector('.tagged-body');
  const showing = body.style.display !== 'none';
  body.style.display = showing ? 'none' : 'block';
  btn.textContent = showing ? 'Show' : 'Hide';
}}

// Build tabs
const tabsEl = document.getElementById('tabs');
const mainEl = document.getElementById('main');

if (RUNS.length === 0) {{
  tabsEl.innerHTML = '<div style="display:flex;align-items:center;padding:0 16px;font-family:\'DM Mono\',monospace;font-size:11px;color:var(--muted);">No runs yet</div>';
}} else {{
  RUNS.forEach((run, i) => {{
    const untagged = run.ads.filter(a => !a.tagged).length;
    const tab = document.createElement('div');
    tab.className = 'tab' + (i===0?' active':'');
    tab.innerHTML = `
      <div class="tab-day">${{run.run_day}}</div>
      <div class="tab-date">${{run.tab_date}}</div>
      <div class="tab-count ${{untagged>0?'has-untagged':''}}">${{untagged>0?'🔴 '+untagged+' action':'✅ all tagged'}}</div>`;
    tab.onclick = () => {{
      document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
      tab.classList.add('active');
      mainEl.innerHTML = renderRun(run);
    }};
    tabsEl.appendChild(tab);
  }});

  // Render most recent run
  mainEl.innerHTML = renderRun(RUNS[0]);
}}
</script>
</body>
</html>'''

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if not ACCESS_TOKEN:
        print("ERROR: META_ACCESS_TOKEN not set"); sys.exit(1)

    ads_raw, date_start, date_end = fetch_ads()
    ads = [parse_ad(a) for a in ads_raw]
    untagged, tagged = classify_ads(ads)
    all_winners = untagged + tagged

    # Fetch creatives
    all_ids = list(set(a['ad_id'] for a in all_winners if a.get('ad_id')))
    print(f"Fetching creatives for {len(all_ids)} ads...")
    creatives = fetch_creatives(all_ids)
    for a in all_winners:
        cr = creatives.get(a.get('ad_id',''), {})
        a['thumbnail'] = cr.get('thumbnail','')
        a['fb_link']   = cr.get('fb_link','')

    # Build run entry
    ds = datetime.strptime(date_start,'%Y-%m-%d').strftime('%-d %b')
    de = datetime.strptime(date_end,  '%Y-%m-%d').strftime('%-d %b %Y')
    tag      = win_tag()
    run_day  = run_label()
    run_date = datetime.now().strftime('%Y-%m-%d')
    tab_date = datetime.now().strftime('%-d %b')

    new_run = {
        'tag':        tag,
        'run_day':    run_day,
        'run_date':   run_date,
        'tab_date':   tab_date,
        'date_range': f"{ds} – {de}",
        'ads':        all_winners,
    }

    # Load + update history
    history = load_history()
    # Remove duplicate if same date already exists
    history['runs'] = [r for r in history['runs'] if r.get('run_date') != run_date]
    history['runs'].insert(0, new_run)
    history['runs'] = history['runs'][:MAX_RUNS]
    save_history(history)

    html = generate_html(history)
    with open('index.html', 'w') as f:
        f.write(html)

    print(f"\n✅ Done — {len(untagged)} needs action | {len(tagged)} tagged | {tag} | {run_day}")
    print(f"   History: {len(history['runs'])} runs stored")
