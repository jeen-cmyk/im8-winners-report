#!/usr/bin/env python3
"""
IM8 Weekly Winners Report — Meta API Auto-Fetcher
Pulls last 7 days of ad data from Meta API, runs winner logic, generates HTML report.
"""

import os, json, re, sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError

# ── CONFIG ──────────────────────────────────────────────────────────────────
ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
AD_ACCOUNT_ID = "act_1000723654649396"
API_VERSION = "v20.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"

ROAS_THRESHOLD = 1.0
PURCHASE_THRESHOLD = 10

WINNER_POOL_KW = ['Winner','Winners','WINNER','TOP30','l7d winner','TOP 50']
ICP_KW = ['ICP','GLP','Menopause','Collagen','ANGLE','ACTIVE SENIOR','Senior',
          'Cognitive','Immune','Fitness','Sleep','Weight','Gut','Joint','Pill',
          'Energy','Green','Young Prof','Persona','NERMW','HCSS','RECOVERY','Aging Athlete','Performance']
L3_EXCL_KW = ['Retargeting','ENGAGER','ATC','GEISTM']

# ── HELPERS ─────────────────────────────────────────────────────────────────
def api_get(path, params):
    params['access_token'] = ACCESS_TOKEN
    url = f"{BASE}/{path}?{urlencode(params)}"
    try:
        req = Request(url, headers={'User-Agent': 'IM8WinnersBot/1.0'})
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except URLError as e:
        print(f"API error: {e}")
        return None

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
        except:
            break
    return results

def get_tier(camp_name):
    c = str(camp_name)
    if c.startswith('XX'): return 'XX'
    if c.startswith('L1'): return 'L1'
    if c.startswith('L2'): return 'L2'
    if c.startswith('L3'): return 'L3'
    return 'OTHER'

def is_winner_pool(adset_name):
    return any(k in str(adset_name) for k in WINNER_POOL_KW)

def is_icp(adset_name):
    return any(k in str(adset_name) for k in ICP_KW)

def is_excl_l3(camp_name):
    return any(k in str(camp_name) for k in L3_EXCL_KW)

def ad_type(name):
    n = str(name).upper()
    if 'KOLUGC' in n: return 'KOL UGC'
    if 'CREATORUGC' in n: return 'Creator UGC'
    if any(x in n for x in ['_VID_','_VSL_','_WOTXT_','_TALKH_']) or n.startswith('VID_'): return 'Video'
    if '_IMG_' in n or n.startswith('IMG_'): return 'Static'
    if '_CAR_' in n: return 'Carousel'
    return 'Other'

def get_note(ad_name, adset_name):
    if is_icp(adset_name): return 'ICP/Persona — WIN tag only, stays in campaign'
    n = str(ad_name).upper()
    if 'KOLUGC' in n: return 'KOL UGC — duplicate to winner pool'
    if 'CREATORUGC' in n: return 'Creator UGC — duplicate to winner pool'
    return 'Generic/Static — duplicate to winner pool'

def note_type(note):
    if 'ICP' in note: return 'icp'
    if 'KOL' in note or 'Creator' in note: return 'kol'
    return 'generic'

def week_tag():
    today = datetime.now()
    # Calculate week number of month
    day = today.day
    week = (day - 1) // 7 + 1
    month_abbr = today.strftime('%b').upper()[:2]  # MA, AP, etc
    year = today.strftime('%y')
    return f"WIN{year}{month_abbr}W{week}"

# ── FETCH ADS ───────────────────────────────────────────────────────────────
def fetch_ads():
    end = datetime.now()
    start = end - timedelta(days=7)
    date_start = start.strftime('%Y-%m-%d')
    date_end = end.strftime('%Y-%m-%d')

    print(f"Fetching ads {date_start} → {date_end} from {AD_ACCOUNT_ID}...")

    fields = ','.join([
        'ad_name', 'campaign_name', 'adset_name',
        'spend', 'purchase_roas', 'actions', 'action_values',
        'ctr', 'website_purchase_roas', 'cost_per_action_type',
        'outbound_clicks_ctr'
    ])

    params = {
        'level': 'ad',
        'fields': fields,
        'time_range': json.dumps({'since': date_start, 'until': date_end}),
        'limit': 500,
    }

    ads = paginate(f"{AD_ACCOUNT_ID}/insights", params)
    print(f"  → {len(ads)} ad rows fetched")
    return ads, date_start, date_end

def parse_ad(ad):
    """Parse raw API response into clean dict."""
    def get_action(actions, action_type):
        if not actions: return 0
        for a in actions:
            if a.get('action_type') == action_type:
                return float(a.get('value', 0))
        return 0

    purchases = get_action(ad.get('actions', []), 'purchase')
    revenue = get_action(ad.get('action_values', []), 'purchase')
    spend = float(ad.get('spend', 0))

    roas_raw = ad.get('purchase_roas', [])
    roas = float(roas_raw[0]['value']) if roas_raw else (revenue / spend if spend > 0 else 0)

    cpa = spend / purchases if purchases > 0 else 0
    ctr = float(ad.get('ctr', 0))

    return {
        'ad_name': ad.get('ad_name', ''),
        'campaign_name': ad.get('campaign_name', ''),
        'adset_name': ad.get('adset_name', ''),
        'spend': round(spend, 2),
        'roas': round(roas, 2),
        'purchases': int(purchases),
        'revenue': round(revenue, 2),
        'cpa': round(cpa, 2),
        'ctr': round(ctr, 2),
        'hook': None,  # Not available via insights API
        'hold': None,
        'url': '',     # Will try to fetch from creative
    }

# ── WINNER LOGIC ────────────────────────────────────────────────────────────
def filter_winners(ads):
    already_tagged = [a for a in ads if 'WIN2' in str(a['ad_name'])]
    tagged_names = {a['ad_name'][:60] for a in already_tagged}

    action_needed = []
    for ad in ads:
        name = ad['ad_name']
        camp = ad['campaign_name']
        adset = ad['adset_name']
        tier = get_tier(camp)

        # Skip already tagged
        if 'WIN2' in str(name): continue
        # Skip non L1/L2/L3
        if tier not in ('L1', 'L2', 'L3'): continue
        # Skip excluded L3s
        if tier == 'L3' and is_excl_l3(camp): continue
        # L1 only: skip winner pool adsets
        if tier == 'L1' and is_winner_pool(adset): continue
        # Skip below threshold
        if ad['roas'] <= ROAS_THRESHOLD: continue
        if ad['purchases'] <= PURCHASE_THRESHOLD: continue
        if ad['spend'] <= 0: continue

        ad['tier'] = tier
        ad['ad_type'] = ad_type(name)
        ad['note'] = get_note(name, adset)
        ad['note_type'] = note_type(ad['note'])
        action_needed.append(ad)

    # Sort: ICP first, then by ROAS desc
    action_needed.sort(key=lambda x: (0 if x['note_type']=='icp' else 1, -x['roas']))
    return action_needed, len(already_tagged)

# ── HTML GENERATION ──────────────────────────────────────────────────────────
def generate_html(winners, tagged_count, date_start, date_end, win_tag):
    icp_ads = [a for a in winners if a['note_type'] == 'icp']
    l1_ads  = [a for a in winners if a['note_type'] != 'icp']

    total_spend   = sum(a['spend'] for a in winners)
    total_revenue = sum(a['revenue'] for a in winners)
    avg_roas      = sum(a['roas'] for a in winners) / max(len(winners), 1)
    total_purch   = sum(a['purchases'] for a in winners)

    # Format dates nicely
    ds = datetime.strptime(date_start, '%Y-%m-%d').strftime('%-d %b')
    de = datetime.strptime(date_end,   '%Y-%m-%d').strftime('%-d %b %Y')
    date_range = f"{ds} – {de}"

    data = {
        'tag': win_tag,
        'week': date_range,
        'action': winners,
        'stats': {
            'action_count': len(winners),
            'icp_count': len(icp_ads),
            'l1_count': len(l1_ads),
            'tagged_count': tagged_count,
            'total_spend': round(total_spend, 2),
            'total_revenue': round(total_revenue, 2),
            'avg_roas': round(avg_roas, 2),
            'total_purchases': total_purch,
        }
    }

    data_json = json.dumps(data)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{win_tag} — IM8 Winners Report</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  :root{{--bg:#0a0a0f;--surface:#111118;--surface2:#18181f;--border:rgba(255,255,255,0.07);--gold:#e8b450;--gold-dim:rgba(232,180,80,0.12);--teal:#3ecfb2;--teal-dim:rgba(62,207,178,0.1);--purple:#9b7cff;--purple-dim:rgba(155,124,255,0.1);--grey:#8888aa;--green:#4ade80;--amber:#fb923c;--white:#f0f0f8;--muted:#5a5a7a;--l1:#fb923c;--l2:#4ade80;--l3:#60a5fa;}}
  *{{margin:0;padding:0;box-sizing:border-box;}}
  body{{background:var(--bg);color:var(--white);font-family:'DM Sans',sans-serif;font-weight:300;min-height:100vh;}}
  body::before{{content:'';position:fixed;inset:0;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.04'/%3E%3C/svg%3E");pointer-events:none;z-index:0;opacity:0.4;}}
  .wrap{{position:relative;z-index:1;max-width:1400px;margin:0 auto;padding:0 32px 80px;}}
  header{{padding:56px 0 48px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;justify-content:space-between;gap:24px;flex-wrap:wrap;}}
  .tag-chip{{display:inline-flex;align-items:center;gap:8px;background:var(--gold-dim);border:1px solid var(--gold);border-radius:4px;padding:5px 14px;font-family:'DM Mono',monospace;font-size:11px;font-weight:500;color:var(--gold);letter-spacing:.12em;text-transform:uppercase;margin-bottom:20px;}}
  .tag-chip::before{{content:'';width:6px;height:6px;background:var(--gold);border-radius:50%;animation:pulse 2s infinite;}}
  @keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.4;transform:scale(.7)}}}}
  h1{{font-family:'Syne',sans-serif;font-size:clamp(36px,5vw,62px);font-weight:800;letter-spacing:-.03em;line-height:1;}}
  h1 span{{color:var(--gold);}}
  .week-label{{margin-top:12px;font-size:13px;color:var(--muted);font-family:'DM Mono',monospace;}}
  .last-updated{{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 16px;align-self:flex-end;}}
  .last-updated strong{{color:var(--green);display:block;margin-bottom:2px;}}
  .stats-grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:40px 0;}}
  .stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px 18px;position:relative;overflow:hidden;animation:fadeUp .5s ease both;}}
  .stat::after{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--gold),transparent);opacity:.5;}}
  .stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:8px;font-family:'DM Mono',monospace;}}
  .stat-value{{font-family:'Syne',sans-serif;font-size:24px;font-weight:700;line-height:1;}}
  .stat-value.gold{{color:var(--gold);}} .stat-value.green{{color:var(--green);}} .stat-value.purple{{color:var(--purple);}}
  .section-head{{display:flex;align-items:center;gap:16px;margin:52px 0 16px;}}
  .section-head h2{{font-family:'Syne',sans-serif;font-size:20px;font-weight:700;letter-spacing:-.02em;}}
  .section-count{{font-family:'DM Mono',monospace;font-size:11px;background:var(--surface2);border:1px solid var(--border);border-radius:20px;padding:3px 12px;color:var(--grey);}}
  .section-line{{flex:1;height:1px;background:var(--border);}}
  .legend{{display:flex;gap:20px;margin-bottom:20px;flex-wrap:wrap;}}
  .legend-item{{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;}}
  .legend-dot{{width:8px;height:8px;border-radius:2px;flex-shrink:0;}}
  .cards{{display:flex;flex-direction:column;gap:8px;}}
  .card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;transition:border-color .2s,transform .2s;animation:fadeUp .4s ease both;cursor:pointer;}}
  .card:hover{{border-color:rgba(232,180,80,.3);transform:translateY(-1px);}}
  .card-main{{display:grid;grid-template-columns:3px 130px 1fr auto;align-items:stretch;min-height:68px;}}
  .card-accent{{width:3px;}} .card-accent.kol{{background:var(--teal);}} .card-accent.generic{{background:var(--gold);}} .card-accent.icp{{background:var(--purple);}}
  .card-left{{display:flex;flex-direction:column;justify-content:center;gap:5px;padding:12px 16px;border-right:1px solid var(--border);flex-shrink:0;}}
  .tier-badge{{display:inline-flex;align-items:center;justify-content:center;font-family:'DM Mono',monospace;font-size:10px;font-weight:500;padding:2px 8px;border-radius:3px;width:fit-content;}}
  .tier-L1{{background:rgba(251,146,60,.15);color:var(--l1);border:1px solid rgba(251,146,60,.3);}}
  .tier-L2{{background:rgba(74,222,128,.1);color:var(--l2);border:1px solid rgba(74,222,128,.25);}}
  .tier-L3{{background:rgba(96,165,250,.1);color:var(--l3);border:1px solid rgba(96,165,250,.25);}}
  .type-chip{{font-size:11px;color:var(--muted);}}
  .action-badge{{font-size:10px;font-family:'DM Mono',monospace;font-weight:500;padding:3px 7px;border-radius:3px;text-align:center;line-height:1.3;}}
  .action-badge.kol{{background:var(--teal-dim);color:var(--teal);border:1px solid rgba(62,207,178,.2);}}
  .action-badge.generic{{background:var(--gold-dim);color:var(--gold);}}
  .action-badge.icp{{background:var(--purple-dim);color:var(--purple);border:1px solid rgba(155,124,255,.2);}}
  .card-body{{display:flex;flex-direction:column;justify-content:center;padding:12px 20px;gap:4px;min-width:0;}}
  .ad-name{{font-family:'DM Mono',monospace;font-size:11px;color:var(--white);opacity:.85;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
  .meta-item{{font-size:11px;color:var(--muted);}}
  .card-metrics{{display:flex;align-items:stretch;flex-shrink:0;border-left:1px solid var(--border);}}
  .metric{{display:flex;flex-direction:column;justify-content:center;align-items:center;padding:0 18px;border-left:1px solid var(--border);gap:3px;min-width:76px;text-align:center;}}
  .metric:first-child{{border-left:none;}}
  .metric-label{{font-size:9px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);font-family:'DM Mono',monospace;}}
  .metric-value{{font-family:'Syne',sans-serif;font-size:17px;font-weight:700;line-height:1;}}
  .metric-value.great{{color:var(--green);}} .metric-value.good{{color:#a3e635;}} .metric-value.ok{{color:var(--amber);}}
  .card-expand{{display:none;border-top:1px solid var(--border);background:var(--surface2);padding:16px 20px;gap:20px;flex-wrap:wrap;}}
  .card.open .card-expand{{display:flex;}}
  .expand-group{{display:flex;flex-direction:column;gap:4px;min-width:120px;}}
  .expand-label{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;font-family:'DM Mono',monospace;}}
  .expand-value{{font-size:12px;color:var(--white);}}
  .expand-value a{{color:var(--gold);text-decoration:none;font-family:'DM Mono',monospace;font-size:11px;word-break:break-all;}}
  .expand-value a:hover{{text-decoration:underline;}}
  .pill-stat{{display:inline-flex;align-items:center;gap:5px;background:var(--bg);border:1px solid var(--border);border-radius:20px;padding:3px 10px;font-size:11px;font-family:'DM Mono',monospace;}}
  .pill-stat .dot{{width:6px;height:6px;border-radius:50%;flex-shrink:0;}}
  .done-btn{{margin-left:auto;align-self:flex-start;background:transparent;border:1px solid var(--border);color:var(--muted);font-family:'DM Mono',monospace;font-size:11px;padding:5px 14px;border-radius:6px;cursor:pointer;transition:all .2s;}}
  .done-btn:hover{{border-color:var(--green);color:var(--green);}}
  .done-btn.done{{background:rgba(74,222,128,.1);border-color:var(--green);color:var(--green);}}
  .card.done-card{{opacity:.4;filter:grayscale(.5);}}
  .divider{{display:flex;align-items:center;gap:12px;margin:24px 0 12px;}}
  .divider-label{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;white-space:nowrap;}}
  .divider-line{{flex:1;height:1px;background:var(--border);}}
  .chevron{{width:20px;height:20px;display:flex;align-items:center;justify-content:center;margin-left:10px;flex-shrink:0;color:var(--muted);transition:transform .2s;}}
  .card.open .chevron{{transform:rotate(180deg);}}
  .empty-state{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:60px 40px;text-align:center;}}
  .empty-state .emoji{{font-size:48px;margin-bottom:16px;display:block;}}
  .empty-state h3{{font-family:'Syne',sans-serif;font-size:22px;font-weight:700;margin-bottom:8px;}}
  .empty-state p{{font-size:13px;color:var(--muted);font-family:'DM Mono',monospace;line-height:1.6;}}
  @keyframes fadeUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
  footer{{margin-top:64px;padding-top:24px;border-top:1px solid var(--border);display:flex;justify-content:space-between;font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;}}
  @media(max-width:900px){{.stats-grid{{grid-template-columns:repeat(3,1fr);}} .card-metrics{{display:none;}}}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <div class="tag-chip">Live Winners Report</div>
      <h1>Winners<br><span>{win_tag}</span></h1>
      <div class="week-label">{date_range} &nbsp;·&nbsp; IM8 Health &nbsp;·&nbsp; Meta Ads &nbsp;·&nbsp; L1 + ICP/Persona</div>
    </div>
    <div class="last-updated">
      <strong>🟢 Auto-updated daily</strong>
      Last run: {datetime.now().strftime('%-d %b %Y, %H:%M')} UTC
    </div>
  </header>
  <div class="stats-grid" id="statsGrid"></div>
  <div class="legend">
    <div class="legend-item"><div class="legend-dot" style="background:var(--teal)"></div>KOL / Creator UGC — Duplicate to winner pool</div>
    <div class="legend-item"><div class="legend-dot" style="background:var(--gold)"></div>Generic/Static — Duplicate to winner pool</div>
    <div class="legend-item"><div class="legend-dot" style="background:var(--purple)"></div>ICP/Persona — WIN tag only, stays in campaign</div>
  </div>
  <div class="section-head">
    <h2>🏷 Action Needed</h2>
    <div class="section-count" id="actionCount"></div>
    <div class="section-line"></div>
    <div style="font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;">Click to expand</div>
  </div>
  <div class="cards" id="actionCards"></div>
  <footer>
    <span>Internal use only &nbsp;·&nbsp; {win_tag} &nbsp;·&nbsp; IM8 Health &nbsp;·&nbsp; Auto-generated via Meta API</span>
    <span id="footerDate"></span>
  </footer>
</div>
<script>
const DATA = {data_json};
const fmt = {{
  usd: v => v!=null&&v>0 ? '$'+v.toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}) : '—',
  roas: v => v!=null ? v.toFixed(2)+'x' : '—',
  num: v => v!=null ? v.toLocaleString() : '—',
}};
function roasClass(r){{return r>=2?'great':r>=1.5?'good':'ok';}}
const stats = DATA.stats;
const statDefs = [
  {{label:'Total Winners', value:stats.action_count, cls:'gold'}},
  {{label:'L1 Discoveries', value:stats.l1_count||0, cls:''}},
  {{label:'ICP/Persona', value:stats.icp_count||0, cls:'purple'}},
  {{label:'Total Spend', value:fmt.usd(stats.total_spend), cls:''}},
  {{label:'Total Revenue', value:fmt.usd(stats.total_revenue), cls:'green'}},
  {{label:'Avg ROAS', value:fmt.roas(stats.avg_roas), cls:stats.avg_roas>=1.5?'green':'gold'}},
];
document.getElementById('statsGrid').innerHTML = statDefs.map((s,i) =>
  `<div class="stat" style="animation-delay:${{i*.05}}s"><div class="stat-label">${{s.label}}</div><div class="stat-value ${{s.cls}}">${{s.value}}</div></div>`
).join('');
document.getElementById('actionCount').textContent = stats.action_count + ' ads';
document.getElementById('footerDate').textContent = 'Generated ' + new Date().toLocaleDateString('en-GB',{{day:'numeric',month:'short',year:'numeric'}});
function buildCard(ad) {{
  const nt = ad.note_type || 'generic';
  const shortNote = nt==='kol' ? 'Dupe → Winner Pool' : nt==='icp' ? 'Tag only' : 'Dupe → Winner Pool';
  return `<div class="card" onclick="toggleCard(this)">
    <div class="card-main">
      <div class="card-accent ${{nt}}"></div>
      <div class="card-left">
        <div class="tier-badge tier-${{ad.tier}}">${{ad.tier}}</div>
        <div class="type-chip">${{ad.ad_type}}</div>
        <div class="action-badge ${{nt}}">${{shortNote}}</div>
      </div>
      <div class="card-body">
        <div class="ad-name">${{ad.ad_name}}</div>
        <div class="meta-item">📁 ${{ad.adset_name}}</div>
      </div>
      <div class="card-metrics">
        <div class="metric"><div class="metric-label">Spend</div><div class="metric-value ok">${{fmt.usd(ad.spend)}}</div></div>
        <div class="metric"><div class="metric-label">ROAS</div><div class="metric-value ${{roasClass(ad.roas)}}">${{fmt.roas(ad.roas)}}</div></div>
        <div class="metric"><div class="metric-label">Purchases</div><div class="metric-value">${{fmt.num(ad.purchases)}}</div></div>
        <div class="metric"><div class="metric-label">Revenue</div><div class="metric-value good">${{fmt.usd(ad.revenue)}}</div></div>
        <div style="display:flex;align-items:center;padding:0 14px;"><div class="chevron"><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 5l5 5 5-5"/></svg></div></div>
      </div>
    </div>
    <div class="card-expand">
      <div class="expand-group"><div class="expand-label">Campaign</div><div class="expand-value">${{ad.campaign_name}}</div></div>
      <div class="expand-group"><div class="expand-label">Ad Set</div><div class="expand-value">${{ad.adset_name}}</div></div>
      <div class="expand-group"><div class="expand-label">Landing Page</div><div class="expand-value"><a href="${{ad.url}}" target="_blank">${{ad.url||'—'}}</a></div></div>
      <div class="expand-group"><div class="expand-label">Metrics</div>
        <div class="expand-value" style="display:flex;gap:8px;flex-wrap:wrap;margin-top:2px;">
          <span class="pill-stat"><span class="dot" style="background:var(--gold)"></span>CTR ${{ad.ctr!=null?ad.ctr+'%':'n/a'}}</span>
          <span class="pill-stat"><span class="dot" style="background:var(--amber)"></span>CPA ${{fmt.usd(ad.cpa)}}</span>
        </div>
      </div>
      <button class="done-btn" onclick="toggleDone(this,event)">☐ Mark done</button>
    </div>
  </div>`;
}}
const icpAds = DATA.action.filter(a => a.note_type === 'icp');
const restAds = DATA.action.filter(a => a.note_type !== 'icp');
const cardsEl = document.getElementById('actionCards');
if (DATA.action.length === 0) {{
  cardsEl.innerHTML = `<div class="empty-state"><span class="emoji">🔍</span><h3>No winners yet this week</h3><p>All ads are below threshold (ROAS &gt; 1.0 &amp; Purchases &gt; 10)<br>Check back tomorrow — this updates automatically.</p></div>`;
}} else {{
  let html = '';
  if (icpAds.length) {{
    html += `<div class="divider"><div class="divider-label">ICP / Persona — tag only</div><div class="divider-line"></div></div>`;
    html += icpAds.map(buildCard).join('');
  }}
  if (restAds.length) {{
    html += `<div class="divider"><div class="divider-label">L1 discoveries — duplicate to winner pool</div><div class="divider-line"></div></div>`;
    html += restAds.map(buildCard).join('');
  }}
  cardsEl.innerHTML = html;
  document.querySelectorAll('.card').forEach((c,i) => c.style.animationDelay = (i*.07)+'s');
}}
function toggleCard(card) {{ card.classList.toggle('open'); }}
function toggleDone(btn, e) {{
  e.stopPropagation();
  const card = btn.closest('.card');
  const done = btn.classList.toggle('done');
  btn.textContent = done ? '✅ Done' : '☐ Mark done';
  card.classList.toggle('done-card', done);
}}
</script>
</body>
</html>'''

# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if not ACCESS_TOKEN:
        print("ERROR: META_ACCESS_TOKEN environment variable not set")
        sys.exit(1)

    ads_raw, date_start, date_end = fetch_ads()
    ads = [parse_ad(a) for a in ads_raw]
    winners, tagged_count = filter_winners(ads)
    win_tag = week_tag()

    print(f"\nResults: {len(winners)} winners | Tag: {win_tag}")
    for w in winners:
        print(f"  [{w['tier']}] ROAS={w['roas']} P={w['purchases']} | {w['ad_name'][:60]}")

    html = generate_html(winners, tagged_count, date_start, date_end, win_tag)

    with open('index.html', 'w') as f:
        f.write(html)
    print(f"\n✅ index.html written ({len(winners)} winners)")
