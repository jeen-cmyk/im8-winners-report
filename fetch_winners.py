#!/usr/bin/env python3
"""
IM8 Weekly Winners Report — Meta API Auto-Fetcher
Runs Mon + Fri. Pulls last 7 days, identifies L1 + ICP winners,
separates new vs returning, generates HTML + WhatsApp summary.
"""

import os, json, re, sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError

# ── CONFIG ───────────────────────────────────────────────────────────────────
ACCESS_TOKEN   = os.environ.get("META_ACCESS_TOKEN", "")
AD_ACCOUNT_ID  = "act_1000723654649396"
API_VERSION    = "v20.0"
BASE           = f"https://graph.facebook.com/{API_VERSION}"
PREV_FILE      = "previous_winners.json"

ROAS_THRESHOLD  = 1.0
PURCH_THRESHOLD = 10

WINNER_POOL_KW = ['Winner','Winners','WINNER','TOP30','l7d winner','TOP 50']
ICP_KW         = ['ICP','GLP','Menopause','Collagen','ANGLE','ACTIVE SENIOR','Senior',
                  'Cognitive','Immune','Fitness','Sleep','Weight','Gut','Joint','Pill',
                  'Energy','Green','Young Prof','Persona','NERMW','HCSS','RECOVERY',
                  'Aging Athlete','Performance','FREQUENTFLYER','Traveler','Travel']
L3_EXCL_KW     = ['Retargeting','ENGAGER','ATC','GEISTM']

# ── HELPERS ──────────────────────────────────────────────────────────────────
def api_get(path, params):
    params['access_token'] = ACCESS_TOKEN
    url = f"{BASE}/{path}?{urlencode(params)}"
    try:
        req = Request(url, headers={'User-Agent': 'IM8WinnersBot/1.0'})
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except URLError as e:
        print(f"API error: {e}"); return None

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
    if s.startswith('XX'): return 'XX'
    if s.startswith('L1'): return 'L1'
    if s.startswith('L2'): return 'L2'
    if s.startswith('L3'): return 'L3'
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
    if is_icp(adset_name): return 'ICP/Persona — WIN tag only, stays in campaign'
    n = str(ad_name).upper()
    if 'KOLUGC' in n or 'KOL_UGC' in n: return 'KOL UGC — duplicate to winner pool'
    if 'CREATORUGC' in n: return 'Creator UGC — duplicate to winner pool'
    return 'Generic/Static — duplicate to winner pool'

def note_type(note):
    if 'ICP' in note: return 'icp'
    if 'KOL' in note or 'Creator' in note: return 'kol'
    return 'generic'

def win_tag():
    d = datetime.now()
    month_abbr = d.strftime('%b').upper()[:2]
    year = d.strftime('%y')
    week = (d.day - 1) // 7 + 1
    return f"WIN{year}{month_abbr}W{week}"

def run_label():
    return "Monday" if datetime.now().weekday() == 0 else "Friday"

# ── FETCH ─────────────────────────────────────────────────────────────────────
def fetch_ads():
    end   = datetime.now()
    start = end - timedelta(days=7)
    date_start = start.strftime('%Y-%m-%d')
    date_end   = end.strftime('%Y-%m-%d')
    print(f"Fetching {date_start} → {date_end}...")
    fields = ','.join(['ad_name','campaign_name','adset_name',
                       'spend','purchase_roas','actions','action_values','ctr'])
    params = {'level':'ad','fields':fields,
              'time_range':json.dumps({'since':date_start,'until':date_end}),'limit':500}
    ads = paginate(f"{AD_ACCOUNT_ID}/insights", params)
    print(f"  → {len(ads)} rows")
    return ads, date_start, date_end

def parse_ad(ad):
    def get_action(actions, t):
        if not actions: return 0
        for a in actions:
            if a.get('action_type') == t: return float(a.get('value', 0))
        return 0
    purchases = get_action(ad.get('actions', []), 'purchase')
    revenue   = get_action(ad.get('action_values', []), 'purchase')
    spend     = float(ad.get('spend', 0))
    roas_raw  = ad.get('purchase_roas', [])
    roas      = float(roas_raw[0]['value']) if roas_raw else (revenue/spend if spend > 0 else 0)
    cpa       = spend / purchases if purchases > 0 else 0
    return {
        'ad_name':       ad.get('ad_name', ''),
        'campaign_name': ad.get('campaign_name', ''),
        'adset_name':    ad.get('adset_name', ''),
        'spend':    round(spend, 2),
        'roas':     round(roas, 2),
        'purchases': int(purchases),
        'revenue':  round(revenue, 2),
        'cpa':      round(cpa, 2),
        'ctr':      round(float(ad.get('ctr', 0)), 2),
        'url':      '',
    }

# ── WINNER LOGIC ──────────────────────────────────────────────────────────────
def filter_winners(ads):
    winners = []; already_tagged = 0
    for ad in ads:
        name  = ad['ad_name']
        camp  = ad['campaign_name']
        adset = ad['adset_name']
        tier  = get_tier(camp)
        if 'WIN2' in str(name): already_tagged += 1; continue
        if tier not in ('L1','L2','L3'): continue
        if tier == 'L3' and is_excl_l3(camp): continue
        if tier == 'L1' and is_winner_pool(adset): continue
        if ad['roas'] <= ROAS_THRESHOLD: continue
        if ad['purchases'] <= PURCH_THRESHOLD: continue
        if ad['spend'] <= 0: continue
        note = get_note(name, adset)
        ad.update({'tier': tier, 'ad_type': ad_type(name),
                   'note': note, 'note_type': note_type(note)})
        winners.append(ad)
    winners.sort(key=lambda x: (0 if x['note_type']=='icp' else 1, -x['roas']))
    return winners, already_tagged

# ── NEW vs RETURNING ──────────────────────────────────────────────────────────
def load_previous():
    if os.path.exists(PREV_FILE):
        try:
            with open(PREV_FILE) as f:
                return set(json.load(f).get('ad_names', []))
        except: pass
    return set()

def save_previous(winners):
    with open(PREV_FILE, 'w') as f:
        json.dump({'ad_names': [w['ad_name'] for w in winners],
                   'run_date': datetime.now().isoformat()}, f)

# ── WHATSAPP SUMMARY ──────────────────────────────────────────────────────────
def extract_creator(ad_name):
    parts = ad_name.upper().split('_')
    role_tags = ['KOL','AMB','ATH','DOC','AFF']
    for i, p in enumerate(parts):
        if p in ('KOLUGC', 'KOL_UGC'):
            ni = i + 1
            if ni < len(parts) and parts[ni] in role_tags: ni += 1
            if ni < len(parts): return parts[ni].title()
    return ''

def whatsapp_summary(winners, tag, date_range, run_day):
    new_w = [w for w in winners if not w.get('returning')]
    ret_w = [w for w in winners if w.get('returning')]
    pool_new = [w for w in new_w if w['note_type'] != 'icp']
    icp_new  = [w for w in new_w if w['note_type'] == 'icp']

    lines = [
        f"🏆 *{tag} Winners — {run_day} {datetime.now().strftime('%-d %b')}*",
        f"📅 {date_range}", "",
    ]

    if pool_new:
        lines.append("*🆕 New — Duplicate to Winner Pool*")
        for w in pool_new:
            creator = extract_creator(w['ad_name'])
            label = f"{w['ad_type']}" + (f" ({creator})" if creator else "")
            lines.append(f"• {label} — ROAS {w['roas']:.2f}x, {w['purchases']}P, ${w['spend']:,.0f}")
        lines.append("")

    if icp_new:
        lines.append("*🆕 New — ICP Tag Only*")
        for w in icp_new:
            lines.append(f"• {w['ad_name'][:50]}... — ROAS {w['roas']:.2f}x, {w['purchases']}P")
        lines.append("")

    if ret_w:
        lines.append("*🔄 Still Winning (prev run)*")
        for w in ret_w:
            creator = extract_creator(w['ad_name'])
            label = f"{w['ad_type']}" + (f" ({creator})" if creator else "")
            lines.append(f"• {label} — ROAS {w['roas']:.2f}x, {w['purchases']}P")
        lines.append("")

    if winners:
        avg_roas  = sum(w['roas'] for w in winners) / len(winners)
        total_rev = sum(w['revenue'] for w in winners)
        total_p   = sum(w['purchases'] for w in winners)
        lines.append(f"📊 *{len(winners)} total | Avg ROAS {avg_roas:.2f}x | {total_p}P | ${total_rev:,.0f} rev*")
    else:
        lines.append("_No winners this window — all ads below threshold_")

    return '\n'.join(lines)

# ── HTML ──────────────────────────────────────────────────────────────────────
def generate_html(winners, tagged_count, date_start, date_end, tag, run_day, wa_text):
    ds = datetime.strptime(date_start, '%Y-%m-%d').strftime('%-d %b')
    de = datetime.strptime(date_end,   '%Y-%m-%d').strftime('%-d %b %Y')
    date_range = f"{ds} – {de}"
    now_str    = datetime.now().strftime('%-d %b %Y, %H:%M UTC')

    pool_new = [w for w in winners if not w.get('returning') and w['note_type'] != 'icp']
    icp_new  = [w for w in winners if not w.get('returning') and w['note_type'] == 'icp']
    returning= [w for w in winners if w.get('returning')]

    stats = {
        'new_pool':    len(pool_new),
        'new_icp':     len(icp_new),
        'returning':   len(returning),
        'total':       len(winners),
        'avg_roas':    round(sum(w['roas'] for w in winners)/max(len(winners),1), 2),
        'total_rev':   round(sum(w['revenue'] for w in winners), 2),
        'total_purch': sum(w['purchases'] for w in winners),
    }

    data_json = json.dumps({
        'tag': tag, 'week': date_range, 'run_day': run_day,
        'now': now_str, 'wa_text': wa_text,
        'pool_new': pool_new, 'icp_new': icp_new, 'returning': returning,
        'stats': stats,
    })

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{tag} — IM8 Winners</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
:root{{--bg:#0a0a0f;--surface:#111118;--surface2:#18181f;--border:rgba(255,255,255,.07);--gold:#e8b450;--gold-dim:rgba(232,180,80,.12);--teal:#3ecfb2;--teal-dim:rgba(62,207,178,.1);--purple:#9b7cff;--purple-dim:rgba(155,124,255,.1);--blue:#60a5fa;--blue-dim:rgba(96,165,250,.1);--green:#4ade80;--amber:#fb923c;--white:#f0f0f8;--muted:#5a5a7a;--l1:#fb923c;--l3:#60a5fa;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:var(--bg);color:var(--white);font-family:"DM Sans",sans-serif;font-weight:300;min-height:100vh;}}
body::before{{content:"";position:fixed;inset:0;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='.04'/%3E%3C/svg%3E");pointer-events:none;z-index:0;opacity:.4;}}
.wrap{{position:relative;z-index:1;max-width:1400px;margin:0 auto;padding:0 32px 80px;}}
header{{padding:48px 0 40px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:flex-end;flex-wrap:wrap;gap:24px;}}
.tag-chip{{display:inline-flex;align-items:center;gap:8px;background:var(--gold-dim);border:1px solid var(--gold);border-radius:4px;padding:4px 12px;font-family:"DM Mono",monospace;font-size:11px;color:var(--gold);letter-spacing:.1em;text-transform:uppercase;margin-bottom:16px;}}
.tag-chip::before{{content:"";width:6px;height:6px;background:var(--gold);border-radius:50%;animation:pulse 2s infinite;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
h1{{font-family:"Syne",sans-serif;font-size:clamp(32px,5vw,56px);font-weight:800;letter-spacing:-.03em;line-height:1;}}
h1 span{{color:var(--gold);}}
.week-label{{margin-top:10px;font-size:13px;color:var(--muted);font-family:"DM Mono",monospace;}}
.run-badge{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 20px;text-align:right;min-width:200px;}}
.run-badge .day{{font-family:"Syne",sans-serif;font-size:20px;font-weight:700;color:var(--green);}}
.run-badge .ts{{font-size:11px;color:var(--muted);font-family:"DM Mono",monospace;margin-top:4px;}}
.run-badge .next{{font-size:11px;color:var(--amber);font-family:"DM Mono",monospace;margin-top:2px;}}
.wa-panel{{background:var(--surface);border:1px solid rgba(37,211,102,.25);border-radius:12px;padding:20px 24px;margin:32px 0;}}
.wa-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;}}
.wa-title{{font-family:"DM Mono",monospace;font-size:12px;color:#25D366;letter-spacing:.08em;text-transform:uppercase;display:flex;align-items:center;gap:8px;}}
.wa-copy{{background:rgba(37,211,102,.1);border:1px solid rgba(37,211,102,.3);color:#25D366;font-family:"DM Mono",monospace;font-size:11px;padding:6px 16px;border-radius:20px;cursor:pointer;transition:all .2s;}}
.wa-copy:hover{{background:rgba(37,211,102,.2);}}
.wa-copy.copied{{background:rgba(37,211,102,.25);}}
.wa-text{{font-family:"DM Mono",monospace;font-size:12px;color:var(--white);opacity:.8;white-space:pre-wrap;line-height:1.75;}}
.stats-grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:32px 0;}}
.stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px;position:relative;overflow:hidden;animation:fadeUp .5s ease both;}}
.stat::after{{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,transparent,var(--gold),transparent);opacity:.5;}}
.stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:6px;font-family:"DM Mono",monospace;}}
.stat-value{{font-family:"Syne",sans-serif;font-size:22px;font-weight:700;}}
.gold{{color:var(--gold);}} .green{{color:var(--green);}} .teal{{color:var(--teal);}} .purple{{color:var(--purple);}} .blue{{color:var(--blue);}}
.section-head{{display:flex;align-items:center;gap:14px;margin:44px 0 12px;}}
.section-head h2{{font-family:"Syne",sans-serif;font-size:17px;font-weight:700;}}
.sc{{font-family:"DM Mono",monospace;font-size:11px;background:var(--surface2);border:1px solid var(--border);border-radius:20px;padding:3px 12px;color:#8888aa;}}
.sl{{flex:1;height:1px;background:var(--border);}}
.sh{{font-size:11px;color:var(--muted);font-family:"DM Mono",monospace;}}
.cards{{display:flex;flex-direction:column;gap:8px;}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;transition:border-color .2s,transform .2s;animation:fadeUp .35s ease both;cursor:pointer;}}
.card:hover{{border-color:rgba(232,180,80,.3);transform:translateY(-1px);}}
.card-main{{display:grid;grid-template-columns:120px 1fr auto;align-items:stretch;min-height:70px;}}
.card-left{{display:flex;flex-direction:column;justify-content:center;gap:5px;padding:12px 14px;border-right:1px solid var(--border);flex-shrink:0;}}
.tb{{display:inline-flex;align-items:center;justify-content:center;font-family:"DM Mono",monospace;font-size:10px;font-weight:500;padding:2px 8px;border-radius:3px;width:fit-content;}}
.tb.tier-L1{{background:rgba(251,146,60,.15);color:var(--l1);border:1px solid rgba(251,146,60,.3);}}
.tb.tier-L3{{background:rgba(96,165,250,.1);color:var(--l3);border:1px solid rgba(96,165,250,.25);}}
.tc{{font-size:10px;color:var(--muted);}}
.ab{{font-size:10px;font-family:"DM Mono",monospace;font-weight:500;padding:3px 7px;border-radius:3px;text-align:center;line-height:1.3;}}
.ab.kol{{background:var(--teal-dim);color:var(--teal);border:1px solid rgba(62,207,178,.2);}}
.ab.generic{{background:var(--gold-dim);color:var(--gold);}}
.ab.icp{{background:var(--purple-dim);color:var(--purple);border:1px solid rgba(155,124,255,.2);}}
.ab.ret{{background:var(--blue-dim);color:var(--blue);border:1px solid rgba(96,165,250,.2);}}
.cb{{display:flex;flex-direction:column;justify-content:center;padding:12px 20px;gap:3px;min-width:0;}}
.an{{font-family:"DM Mono",monospace;font-size:11px;color:var(--white);opacity:.85;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.mr{{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-top:2px;}}
.mi{{font-size:11px;color:var(--muted);}}
.im{{font-family:"DM Mono",monospace;font-size:11px;font-weight:500;}}
.im.sp{{color:var(--amber);}} .im.pu{{color:var(--white);opacity:.6;}}
.im.ro.great{{color:var(--green);font-weight:700;}} .im.ro.good{{color:#a3e635;}} .im.ro.ok{{color:var(--amber);}}
.ce{{display:none;border-top:1px solid var(--border);background:var(--surface2);padding:14px 20px;gap:18px;flex-wrap:wrap;}}
.card.open .ce{{display:flex;}}
.eg{{display:flex;flex-direction:column;gap:3px;min-width:110px;}}
.el{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;font-family:"DM Mono",monospace;}}
.ev{{font-size:12px;color:var(--white);}}
.ev a{{color:var(--gold);text-decoration:none;font-family:"DM Mono",monospace;font-size:11px;word-break:break-all;}}
.ev a:hover{{text-decoration:underline;}}
.db{{margin-left:auto;align-self:flex-start;background:transparent;border:1px solid var(--border);color:var(--muted);font-family:"DM Mono",monospace;font-size:11px;padding:5px 14px;border-radius:6px;cursor:pointer;transition:all .2s;}}
.db:hover{{border-color:var(--green);color:var(--green);}}
.db.done{{background:rgba(74,222,128,.1);border-color:var(--green);color:var(--green);}}
.card.done-card{{opacity:.4;filter:grayscale(.5);}}
.ch{{width:20px;height:20px;display:flex;align-items:center;justify-content:center;margin:0 14px;flex-shrink:0;color:var(--muted);transition:transform .2s;}}
.card.open .ch{{transform:rotate(180deg);}}
.empty{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:32px;text-align:center;font-family:"DM Mono",monospace;font-size:12px;color:var(--muted);}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:translateY(0)}}}}
footer{{margin-top:64px;padding-top:24px;border-top:1px solid var(--border);display:flex;justify-content:space-between;font-size:11px;color:var(--muted);font-family:"DM Mono",monospace;}}
@media(max-width:900px){{.stats-grid{{grid-template-columns:repeat(3,1fr);}}}}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <div class="tag-chip">IM8 Winners Report</div>
      <h1>Winners<br><span>{tag}</span></h1>
      <div class="week-label">{date_range} &nbsp;·&nbsp; IM8 Health &nbsp;·&nbsp; Meta Ads</div>
    </div>
    <div class="run-badge">
      <div class="day">{run_day} Cut</div>
      <div class="ts">Updated: {now_str}</div>
      <div class="next">Next: <span id="nextRun"></span></div>
    </div>
  </header>

  <div class="wa-panel">
    <div class="wa-header">
      <div class="wa-title">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="#25D366"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/><path d="M12 0C5.373 0 0 5.373 0 12c0 2.087.537 4.046 1.477 5.755L.055 23.438a.5.5 0 0 0 .614.6l5.7-1.494A11.955 11.955 0 0 0 12 24c6.627 0 12-5.373 12-12S18.627 0 12 0zm0 21.75a9.742 9.742 0 0 1-5.031-1.394l-.36-.214-3.735.978.997-3.634-.234-.374A9.719 9.719 0 0 1 2.25 12C2.25 6.615 6.615 2.25 12 2.25S21.75 6.615 21.75 12 17.385 21.75 12 21.75z"/></svg>
        WhatsApp Summary
      </div>
      <button class="wa-copy" onclick="copyWA(this)">Copy</button>
    </div>
    <div class="wa-text" id="waText"></div>
  </div>

  <div class="stats-grid" id="sg"></div>

  <div class="section-head">
    <h2>🆕 New — Duplicate to Winner Pool</h2>
    <div class="sc" id="c1"></div><div class="sl"></div>
  </div>
  <div class="cards" id="s1"></div>

  <div class="section-head">
    <h2>🆕 New — ICP Tag Only</h2>
    <div class="sc" id="c2"></div><div class="sl"></div>
  </div>
  <div class="cards" id="s2"></div>

  <div class="section-head">
    <h2>🔄 Still Winning</h2>
    <div class="sc" id="c3"></div><div class="sl"></div>
    <div class="sh">From last run — no action needed</div>
  </div>
  <div class="cards" id="s3"></div>

  <footer>
    <span>Internal use only &nbsp;·&nbsp; {tag} &nbsp;·&nbsp; IM8 Health &nbsp;·&nbsp; Auto-generated via Meta API</span>
    <span>{now_str}</span>
  </footer>
</div>
<script>
const D={data_json};
const f={{u:v=>v>0?'$'+v.toLocaleString('en-US',{{minimumFractionDigits:0,maximumFractionDigits:0}}):'—',r:v=>v.toFixed(2)+'x'}};

// Next run label
const dow=new Date().getDay();
const next=new Date();
next.setDate(next.getDate()+(dow===1?4:dow===5?3:((8-dow)%7||7)));
document.getElementById('nextRun').textContent=next.toLocaleDateString('en-GB',{{weekday:'short',day:'numeric',month:'short'}});

// WA
document.getElementById('waText').textContent=D.wa_text;
function copyWA(btn){{navigator.clipboard.writeText(D.wa_text).then(()=>{{btn.textContent='Copied ✓';btn.classList.add('copied');setTimeout(()=>{{btn.textContent='Copy';btn.classList.remove('copied');}},2000);}});}}

// Stats
const s=D.stats;
document.getElementById('sg').innerHTML=[
  {{l:'New (Pool)',v:s.new_pool,c:'teal'}},{{l:'New (ICP)',v:s.new_icp,c:'purple'}},
  {{l:'Still Winning',v:s.returning,c:'blue'}},{{l:'Total',v:s.total,c:'gold'}},
  {{l:'Revenue',v:f.u(s.total_rev),c:'green'}},
  {{l:'Avg ROAS',v:f.r(s.avg_roas),c:s.avg_roas>=1.5?'green':'gold'}},
].map((x,i)=>`<div class="stat" style="animation-delay:${{i*.05}}s"><div class="stat-label">${{x.l}}</div><div class="stat-value ${{x.c}}">${{x.v}}</div></div>`).join('');

function rc(r){{return r>=2?'great':r>=1.5?'good':'ok';}}
function card(ad,isRet){{
  const nt=ad.note_type||'generic';
  const badge=isRet?'Still Winning':nt==='kol'?'Dupe → Pool':nt==='icp'?'Tag only':'Dupe → Pool';
  const bc=isRet?'ret':nt;
  return `<div class="card" onclick="tog(this)">
    <div class="card-main">
      <div class="card-left"><div class="tb tier-${{ad.tier}}">${{ad.tier}}</div><div class="tc">${{ad.ad_type}}</div><div class="ab ${{bc}}">${{badge}}</div></div>
      <div class="cb">
        <div class="an">${{ad.ad_name}}</div>
        <div class="mr"><span class="mi">📁 ${{ad.adset_name}}</span></div>
        <div class="mr"><span class="im sp">${{f.u(ad.spend)}}</span><span class="im ro ${{rc(ad.roas)}}">ROAS ${{f.r(ad.roas)}}</span><span class="im pu">${{ad.purchases}} purchases</span></div>
      </div>
      <div class="ch"><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2"><path d="M2 5l5 5 5-5"/></svg></div>
    </div>
    <div class="ce">
      <div class="eg"><div class="el">Campaign</div><div class="ev">${{ad.campaign_name}}</div></div>
      <div class="eg"><div class="el">Revenue</div><div class="ev" style="color:var(--green);font-family:'DM Mono',monospace;">${{f.u(ad.revenue)}}</div></div>
      <div class="eg"><div class="el">CPA</div><div class="ev" style="font-family:'DM Mono',monospace;">${{f.u(ad.cpa)}}</div></div>
      <div class="eg"><div class="el">Landing Page</div><div class="ev"><a href="${{ad.url}}" target="_blank">${{ad.url||'—'}}</a></div></div>
      <button class="db" onclick="done(this,event)">☐ Mark done</button>
    </div>
  </div>`;
}}
function render(id,cnt,ads,isRet){{
  document.getElementById(cnt).textContent=ads.length+' ads';
  document.getElementById(id).innerHTML=ads.length?ads.map(a=>card(a,isRet)).join(''):'<div class="empty">None this window ✓</div>';
}}
render('s1','c1',D.pool_new,false);
render('s2','c2',D.icp_new,false);
render('s3','c3',D.returning,true);
document.querySelectorAll('.card').forEach((c,i)=>c.style.animationDelay=(i*.04)+'s');
function tog(c){{c.classList.toggle('open');}}
function done(btn,e){{e.stopPropagation();const c=btn.closest('.card');const d=btn.classList.toggle('done');btn.textContent=d?'✅ Done':'☐ Mark done';c.classList.toggle('done-card',d);}}
</script>
</body>
</html>'''

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if not ACCESS_TOKEN:
        print("ERROR: META_ACCESS_TOKEN not set"); sys.exit(1)

    ads_raw, date_start, date_end = fetch_ads()
    ads = [parse_ad(a) for a in ads_raw]
    winners, tagged_count = filter_winners(ads)

    prev_names = load_previous()
    for w in winners:
        w['returning'] = w['ad_name'] in prev_names

    tag      = win_tag()
    run_day  = run_label()
    ds = datetime.strptime(date_start,'%Y-%m-%d').strftime('%-d %b')
    de = datetime.strptime(date_end,  '%Y-%m-%d').strftime('%-d %b %Y')
    date_range = f"{ds} – {de}"

    wa_text = whatsapp_summary(winners, tag, date_range, run_day)
    html    = generate_html(winners, tagged_count, date_start, date_end, tag, run_day, wa_text)

    with open('index.html', 'w') as f:
        f.write(html)
    save_previous(winners)

    new_c = sum(1 for w in winners if not w['returning'])
    ret_c = sum(1 for w in winners if w['returning'])
    print(f"\n✅ {len(winners)} winners | {tag} | {run_day} | New: {new_c} | Returning: {ret_c}")
    print(f"\nWhatsApp preview:\n{wa_text}")
