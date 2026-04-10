#!/usr/bin/env python3
"""
IM8 Winners Dashboard — Meta API
Pulls last 30 days, stores all qualifying ads, generates filterable dashboard.
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
        req = Request(url, headers={'User-Agent':'IM8WinnersBot/1.0'})
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
            req = Request(data['paging']['next'], headers={'User-Agent':'IM8WinnersBot/1.0'})
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

def fetch_ads(days=30):
    end   = datetime.now()
    start = end - timedelta(days=days)
    ds, de = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
    print(f"Fetching {ds} → {de} ({days}d)...")
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
        'spend':    round(spend,2),   'roas':     round(roas,2),
        'purchases': int(purchases),  'revenue':  round(revenue,2),
        'cpa':      round(spend/purchases,2) if purchases>0 else 0,
        'ctr':      round(float(ad.get('ctr',0)),2),
        'thumbnail':'', 'fb_link':'',
    }

def classify_ads(ads):
    result = []
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
        ad.update({'tier':tier, 'ad_type':ad_type(name),
                   'note':note, 'note_type':note_type(note),
                   'lp':get_lp(name), 'tagged':is_tagged})
        result.append(ad)
    result.sort(key=lambda x: (x['tagged'], 0 if x['note_type']=='icp' else 1, -x['roas']))
    return result

def fetch_creatives(ad_ids):
    if not ad_ids: return {}
    creatives = {}
    for i in range(0, len(ad_ids), 50):
        batch = ad_ids[i:i+50]
        data = api_get(f"{AD_ACCOUNT_ID}/ads", {
            'fields':'id,creative{thumbnail_url,effective_object_story_id}',
            'filtering':json.dumps([{'field':'id','operator':'IN','value':batch}]),
            'limit':50,
        })
        if not data: continue
        for ad in data.get('data', []):
            aid = ad.get('id','')
            cr  = ad.get('creative', {})
            thumb    = cr.get('thumbnail_url','')
            story_id = cr.get('effective_object_story_id','')
            fb_link  = ''
            if story_id:
                parts = story_id.split('_', 1)
                if len(parts) == 2:
                    fb_link = f"https://www.facebook.com/permalink.php?story_fbid={parts[1]}&id={parts[0]}"
            creatives[aid] = {'thumbnail':thumb, 'fb_link':fb_link}
    print(f"  → Creatives: {len(creatives)}")
    return creatives

def generate_html(ads, date_start, date_end):
    now_str   = datetime.now().strftime('%-d %b %Y, %H:%M UTC')
    untagged  = [a for a in ads if not a['tagged']]
    tagged    = [a for a in ads if a['tagged']]
    ads_json  = json.dumps(ads)
    ds_iso    = date_start
    de_iso    = date_end

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>IM8 Winners</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0a0a0f;--surface:#111118;--surface2:#18181f;--border:rgba(255,255,255,.07);--gold:#e8b450;--gold-dim:rgba(232,180,80,.12);--teal:#3ecfb2;--teal-dim:rgba(62,207,178,.1);--purple:#9b7cff;--purple-dim:rgba(155,124,255,.1);--blue:#60a5fa;--green:#4ade80;--amber:#fb923c;--white:#f0f0f8;--muted:#5a5a7a;--l1:#fb923c;--l3:#60a5fa;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:var(--bg);color:var(--white);font-family:"DM Sans",sans-serif;font-weight:300;min-height:100vh;}}
body::before{{content:"";position:fixed;inset:0;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='.04'/%3E%3C/svg%3E");pointer-events:none;z-index:0;opacity:.4;}}
.app{{position:relative;z-index:1;}}

/* Header */
.header{{background:var(--surface);border-bottom:1px solid var(--border);padding:20px 32px;position:sticky;top:0;z-index:100;}}
.header-top{{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;}}
.brand{{display:flex;align-items:center;gap:10px;}}
.brand-dot{{width:8px;height:8px;background:var(--gold);border-radius:50%;animation:pulse 2s infinite;flex-shrink:0;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.brand-name{{font-family:"Syne",sans-serif;font-size:16px;font-weight:800;color:var(--gold);}}
.header-meta{{font-family:"DM Mono",monospace;font-size:11px;color:var(--muted);display:flex;align-items:center;gap:12px;}}
.live-dot{{width:6px;height:6px;background:var(--green);border-radius:50%;animation:pulse 2s infinite;display:inline-block;margin-right:4px;}}

/* Filter bar */
.filters{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}}
.filter-group{{display:flex;align-items:center;gap:6px;}}
.filter-label{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;white-space:nowrap;}}
.date-input{{background:var(--surface2);border:1px solid var(--border);color:var(--white);font-family:"DM Mono",monospace;font-size:11px;padding:6px 10px;border-radius:8px;outline:none;cursor:pointer;}}
.date-input:focus{{border-color:var(--gold);}}
.sep{{font-family:"DM Mono",monospace;font-size:11px;color:var(--muted);}}
.pill-group{{display:flex;gap:4px;}}
.pill{{background:var(--surface2);border:1px solid var(--border);color:var(--muted);font-family:"DM Mono",monospace;font-size:11px;padding:5px 12px;border-radius:20px;cursor:pointer;transition:all .15s;white-space:nowrap;}}
.pill:hover{{border-color:var(--gold);color:var(--gold);}}
.pill.active{{background:var(--gold-dim);border-color:var(--gold);color:var(--gold);}}
.pill.action-pill.active{{background:rgba(251,146,60,.15);border-color:var(--amber);color:var(--amber);}}
.pill.tagged-pill.active{{background:rgba(74,222,128,.1);border-color:var(--green);color:var(--green);}}
.divider{{width:1px;height:24px;background:var(--border);}}
.results-count{{font-family:"DM Mono",monospace;font-size:11px;color:var(--muted);margin-left:auto;white-space:nowrap;}}
.results-count b{{color:var(--white);}}

/* Main */
.main{{padding:24px 32px;max-width:1600px;margin:0 auto;}}

/* Stats */
.stats-row{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:24px;}}
.stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;position:relative;overflow:hidden;}}
.stat::after{{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--gold),transparent);opacity:.4;}}
.stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:5px;font-family:"DM Mono",monospace;}}
.stat-value{{font-family:"Syne",sans-serif;font-size:22px;font-weight:700;}}

/* Cards */
.cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px;}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;transition:border-color .2s,transform .2s;animation:fadeUp .25s ease both;}}
.card:hover{{border-color:rgba(232,180,80,.3);transform:translateY(-2px);box-shadow:0 8px 32px rgba(0,0,0,.3);}}
.card.tagged-card{{border-left:3px solid var(--green);}}
.card.untagged-card{{border-left:3px solid var(--amber);}}

/* Thumbnail */
.thumb{{position:relative;width:100%;aspect-ratio:16/9;background:var(--surface2);overflow:hidden;}}
.thumb img{{width:100%;height:100%;object-fit:cover;display:block;transition:transform .3s;}}
.card:hover .thumb img{{transform:scale(1.02);}}
.no-thumb{{display:flex;align-items:center;justify-content:center;width:100%;height:100%;color:var(--muted);font-family:"DM Mono",monospace;font-size:11px;flex-direction:column;gap:8px;}}
.thumb-tl{{position:absolute;top:8px;left:8px;display:flex;gap:5px;}}
.thumb-tr{{position:absolute;top:8px;right:8px;display:flex;gap:5px;}}
.thumb-bl{{position:absolute;bottom:8px;left:8px;}}
.thumb-br{{position:absolute;bottom:8px;right:8px;}}
.badge-thumb{{font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:3px 8px;border-radius:5px;backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);}}
.badge-thumb.kol{{background:rgba(62,207,178,.85);color:#000;}}
.badge-thumb.icp{{background:rgba(155,124,255,.85);color:#fff;}}
.badge-thumb.generic{{background:rgba(232,180,80,.85);color:#000;}}
.badge-thumb.tagged{{background:rgba(74,222,128,.9);color:#000;font-weight:700;}}
.badge-thumb.untagged{{background:rgba(251,146,60,.9);color:#000;font-weight:700;}}
.fb-btn{{display:flex;align-items:center;gap:5px;background:rgba(10,10,20,.75);border:1px solid rgba(96,165,250,.3);border-radius:6px;padding:5px 10px;font-family:"DM Mono",monospace;font-size:10px;color:var(--blue);text-decoration:none;backdrop-filter:blur(8px);transition:all .2s;}}
.fb-btn:hover{{background:rgba(96,165,250,.2);border-color:var(--blue);}}

/* Card body */
.card-body{{padding:14px 16px;}}
.ad-name{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:10px;}}
.metrics{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px;}}
.m{{display:flex;flex-direction:column;gap:2px;}}
.m-label{{font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-family:"DM Mono",monospace;}}
.m-value{{font-family:"Syne",sans-serif;font-size:17px;font-weight:700;line-height:1;}}
.m-value.great{{color:var(--green);}} .m-value.good{{color:#a3e635;}} .m-value.ok{{color:var(--amber);}}
.card-foot{{display:flex;align-items:center;gap:6px;flex-wrap:wrap;}}
.badge{{font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:3px 8px;border-radius:5px;}}
.badge.tier-L1{{background:rgba(251,146,60,.15);color:var(--l1);border:1px solid rgba(251,146,60,.3);}}
.badge.tier-L3{{background:rgba(96,165,250,.1);color:var(--l3);border:1px solid rgba(96,165,250,.2);}}
.badge.kol{{background:var(--teal-dim);color:var(--teal);border:1px solid rgba(62,207,178,.2);}}
.badge.icp{{background:var(--purple-dim);color:var(--purple);border:1px solid rgba(155,124,255,.2);}}
.badge.generic{{background:var(--gold-dim);color:var(--gold);}}
.lp-link{{font-family:"DM Mono",monospace;font-size:10px;color:var(--muted);text-decoration:none;margin-left:auto;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.lp-link:hover{{color:var(--gold);}}

/* Empty */
.empty{{display:flex;flex-direction:column;align-items:center;justify-content:center;padding:80px 40px;text-align:center;border:1px dashed var(--border);border-radius:14px;}}
.empty .e-icon{{font-size:40px;margin-bottom:12px;}}
.empty h3{{font-family:"Syne",sans-serif;font-size:20px;font-weight:700;margin-bottom:6px;}}
.empty p{{font-family:"DM Mono",monospace;font-size:11px;color:var(--muted);}}

@keyframes fadeUp{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:translateY(0)}}}}
@media(max-width:768px){{
  .header{{padding:16px;}} .main{{padding:16px;}}
  .stats-row{{grid-template-columns:repeat(3,1fr);}}
  .cards-grid{{grid-template-columns:1fr;}}
  .filters{{gap:8px;}}
}}
</style>
</head>
<body>
<div class="app">
<div class="header">
  <div class="header-top">
    <div class="brand">
      <div class="brand-dot"></div>
      <div class="brand-name">IM8 Winners</div>
    </div>
    <div class="header-meta">
      <span><span class="live-dot"></span>Live</span>
      <span>Updated: {now_str}</span>
      <span>Data: {date_start} → {date_end}</span>
    </div>
  </div>
  <div class="filters">
    <div class="filter-group">
      <span class="filter-label">Date</span>
      <input type="date" class="date-input" id="dateFrom" onchange="applyFilters()">
      <span class="sep">→</span>
      <input type="date" class="date-input" id="dateTo" onchange="applyFilters()">
    </div>
    <div class="divider"></div>
    <div class="filter-group">
      <span class="filter-label">Status</span>
      <div class="pill-group">
        <button class="pill active" data-filter="status" data-val="all" onclick="setPill(this,'status')">All</button>
        <button class="pill action-pill" data-filter="status" data-val="untagged" onclick="setPill(this,'status')">🔴 Needs Action</button>
        <button class="pill tagged-pill" data-filter="status" data-val="tagged" onclick="setPill(this,'status')">✅ Tagged</button>
      </div>
    </div>
    <div class="divider"></div>
    <div class="filter-group">
      <span class="filter-label">Format</span>
      <div class="pill-group">
        <button class="pill active" data-filter="type" data-val="all" onclick="setPill(this,'type')">All</button>
        <button class="pill" data-filter="type" data-val="KOL UGC" onclick="setPill(this,'type')">KOL UGC</button>
        <button class="pill" data-filter="type" data-val="Static" onclick="setPill(this,'type')">Static</button>
        <button class="pill" data-filter="type" data-val="Video" onclick="setPill(this,'type')">Video</button>
        <button class="pill" data-filter="type" data-val="IG Post" onclick="setPill(this,'type')">IG Post</button>
        <button class="pill" data-filter="type" data-val="Creator UGC" onclick="setPill(this,'type')">Creator UGC</button>
      </div>
    </div>
    <div class="divider"></div>
    <div class="filter-group">
      <span class="filter-label">Tier</span>
      <div class="pill-group">
        <button class="pill active" data-filter="tier" data-val="all" onclick="setPill(this,'tier')">All</button>
        <button class="pill" data-filter="tier" data-val="L1" onclick="setPill(this,'tier')">L1</button>
        <button class="pill" data-filter="tier" data-val="L3" onclick="setPill(this,'tier')">L3</button>
      </div>
    </div>
    <div class="results-count" id="resultsCount"></div>
  </div>
</div>

<div class="main">
  <div class="stats-row" id="statsRow"></div>
  <div class="cards-grid" id="cardsGrid"></div>
</div>
</div>

<script>
const ALL_ADS = {ads_json};
const DATE_START = '{ds_iso}';
const DATE_END   = '{de_iso}';

// Init date pickers to full range
document.getElementById('dateFrom').value = DATE_START;
document.getElementById('dateTo').value   = DATE_END;

const state = {{ status:'all', type:'all', tier:'all' }};

function setPill(btn, group) {{
  document.querySelectorAll(`[data-filter="${{group}}"]`).forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  state[group] = btn.dataset.val;
  applyFilters();
}}

function applyFilters() {{
  const from = document.getElementById('dateFrom').value;
  const to   = document.getElementById('dateTo').value;

  let filtered = ALL_ADS.filter(ad => {{
    if (state.status === 'untagged' && ad.tagged)   return false;
    if (state.status === 'tagged'   && !ad.tagged)  return false;
    if (state.type !== 'all' && ad.ad_type !== state.type) return false;
    if (state.tier !== 'all' && ad.tier   !== state.tier)  return false;
    return true;
  }});

  renderStats(filtered);
  renderCards(filtered);
  document.getElementById('resultsCount').innerHTML = `<b>${{filtered.length}}</b> ads`;
}}

function renderStats(ads) {{
  const untagged = ads.filter(a=>!a.tagged).length;
  const tagged   = ads.filter(a=>a.tagged).length;
  const avgRoas  = ads.length ? (ads.reduce((s,a)=>s+a.roas,0)/ads.length).toFixed(2) : '—';
  const totalRev = ads.reduce((s,a)=>s+a.revenue,0);
  const totalP   = ads.reduce((s,a)=>s+a.purchases,0);
  document.getElementById('statsRow').innerHTML = [
    {{l:'Needs Action', v:untagged, c:'var(--amber)'}},
    {{l:'Already Tagged', v:tagged, c:'var(--green)'}},
    {{l:'Avg ROAS', v:avgRoas+'x', c:'var(--gold)'}},
    {{l:'Total Purchases', v:totalP.toLocaleString(), c:'var(--white)'}},
    {{l:'Revenue', v:totalRev>0?'$'+totalRev.toLocaleString('en-US',{{maximumFractionDigits:0}}):'—', c:'var(--green)'}},
  ].map(s=>`<div class="stat"><div class="stat-label">${{s.l}}</div><div class="stat-value" style="color:${{s.c}}">${{s.v}}</div></div>`).join('');
}}

function roasCls(r) {{ return r>=2?'great':r>=1.5?'good':'ok'; }}

function buildCard(ad) {{
  const nt = ad.note_type || 'generic';
  const thumb = ad.thumbnail
    ? `<img src="${{ad.thumbnail}}" alt="" loading="lazy" onerror="this.parentElement.innerHTML='<div class=\\"no-thumb\\"><span>📷</span><span>No preview</span></div>'">`
    : `<div class="no-thumb"><span>📷</span><span>No preview</span></div>`;
  const fbBtn = ad.fb_link
    ? `<a class="fb-btn" href="${{ad.fb_link}}" target="_blank" onclick="event.stopPropagation()">
        <svg width="11" height="11" viewBox="0 0 24 24" fill="currentColor"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>
        View Post
       </a>`
    : '';
  const lp = ad.lp
    ? `<a class="lp-link" href="${{ad.lp}}" target="_blank" onclick="event.stopPropagation()" title="${{ad.lp}}">${{ad.lp.replace(/https?:\/\//,'').replace('get.im8health.com','↗ get.im8').replace('im8health.com','↗ im8')}}</a>`
    : '';
  const statusBadge = ad.tagged
    ? `<span class="badge-thumb tagged">✓ WIN Tagged</span>`
    : `<span class="badge-thumb untagged">⚡ Action</span>`;

  return `<div class="card ${{ad.tagged?'tagged-card':'untagged-card'}}">
    <div class="thumb">
      ${{thumb}}
      <div class="thumb-tl"><span class="badge-thumb ${{nt}}">${{ad.ad_type}}</span></div>
      <div class="thumb-tr">${{statusBadge}}</div>
      <div class="thumb-br">${{fbBtn}}</div>
    </div>
    <div class="card-body">
      <div class="ad-name" title="${{ad.ad_name}}">${{ad.ad_name}}</div>
      <div class="metrics">
        <div class="m"><div class="m-label">ROAS</div><div class="m-value ${{roasCls(ad.roas)}}">${{ad.roas.toFixed(2)}}x</div></div>
        <div class="m"><div class="m-label">Purchases</div><div class="m-value">${{ad.purchases}}</div></div>
        <div class="m"><div class="m-label">Spend</div><div class="m-value" style="font-size:13px;color:var(--amber)">${{ad.spend>0?'$'+ad.spend.toLocaleString('en-US',{{maximumFractionDigits:0}}):' —'}}</div></div>
        <div class="m"><div class="m-label">Revenue</div><div class="m-value" style="font-size:13px;color:var(--green)">${{ad.revenue>0?'$'+ad.revenue.toLocaleString('en-US',{{maximumFractionDigits:0}}):' —'}}</div></div>
      </div>
      <div class="card-foot">
        <span class="badge tier-${{ad.tier}}">${{ad.tier}}</span>
        <span class="badge ${{nt}}">${{ad.note}}</span>
        ${{lp}}
      </div>
    </div>
  </div>`;
}}

function renderCards(ads) {{
  const grid = document.getElementById('cardsGrid');
  if (!ads.length) {{
    grid.innerHTML = `<div class="empty" style="grid-column:1/-1">
      <div class="e-icon">🔍</div>
      <h3>No ads match</h3>
      <p>Try adjusting the filters or date range</p>
    </div>`;
    return;
  }}
  // Untagged first, then tagged
  const sorted = [...ads.filter(a=>!a.tagged), ...ads.filter(a=>a.tagged)];
  grid.innerHTML = sorted.map((a,i) => {{
    const card = buildCard(a);
    return card.replace('<div class="card', `<div class="card" style="animation-delay:${{Math.min(i,20)*.03}}s`);
  }}).join('');
}}

// Initial render
applyFilters();
</script>
</body>
</html>'''

if __name__ == '__main__':
    if not ACCESS_TOKEN:
        print("ERROR: META_ACCESS_TOKEN not set"); sys.exit(1)

    ads_raw, date_start, date_end = fetch_ads(days=30)
    ads = [parse_ad(a) for a in ads_raw]
    classified = classify_ads(ads)

    print(f"Fetching creatives for {len(classified)} qualifying ads...")
    all_ids = list(set(a['ad_id'] for a in classified if a.get('ad_id')))
    creatives = fetch_creatives(all_ids)
    for a in classified:
        cr = creatives.get(a.get('ad_id',''), {})
        a['thumbnail'] = cr.get('thumbnail','')
        a['fb_link']   = cr.get('fb_link','')

    html = generate_html(classified, date_start, date_end)
    with open('index.html', 'w') as f:
        f.write(html)

    untagged = sum(1 for a in classified if not a['tagged'])
    tagged   = sum(1 for a in classified if a['tagged'])
    print(f"\n✅ Done — {untagged} needs action | {tagged} tagged | {len(classified)} total")
