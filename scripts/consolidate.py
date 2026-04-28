#!/usr/bin/env python3
"""
Consolidate data sources into public.json, hybrid.json, meta.json
for the 增购地图Agent tool.
"""

import json
import re
import os
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# ── File paths ───────────────────────────────────────────────
BASE = '/Users/sarading/Downloads'
F_PUBLIC   = f'{BASE}/公共云潜力客户.xlsx'
F_HYBRID   = f'{BASE}/QBI混合云增购地图.csv'
F_LEGACY   = f'{BASE}/高潜力客户名单_按部门分sheet_261家_小Q标注.xlsx'
F_DETAIL   = f'{BASE}/华东高潜力客户底池.xlsx'
F_INDUST   = f'{BASE}/高潜力客户合并名单_按行业分类.xlsx'
F_XIAOQ    = f'{BASE}/小q数据.xlsx'
F_HUADONG  = f'{BASE}/华东客户群.xlsx'

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
OUT_PUBLIC  = os.path.join(DATA_DIR, 'public.json')
OUT_HYBRID  = os.path.join(DATA_DIR, 'hybrid.json')
OUT_META    = os.path.join(DATA_DIR, 'meta.json')

# ── Helper functions ─────────────────────────────────────────
def parse_module_pv(val):
    """Parse '["仪表板:31","电子表格:142"]' into dict."""
    if not val or pd.isna(val) or str(val).strip() in ('', '-', 'nan', 'None'):
        return {}
    s = str(val)
    pairs = re.findall(r'"([^"]+):(\d+)"', s)
    if not pairs:
        pairs = re.findall(r'([^\[\]",: ]+):(\d+)', s)
    return {k.strip(): int(v) for k, v in pairs}

def safe_num(v, default=None):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        s = str(v).replace(',', '').replace('万', '').strip()
        if s in ('', '-', 'nan', 'None'):
            return default
        return float(s)
    except:
        return default

def safe_str(v, default=''):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    s = str(v).strip()
    return default if s in ('nan', 'None', '-') else s

def safe_pct(v, default=None):
    n = safe_num(v)
    if n is None:
        return default
    return round(n * 100, 1) if n <= 1 else round(n, 1)

def fmt_date(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().split('.')[0]
    if len(s) == 8 and s.isdigit():
        return f'{s[:4]}-{s[4:6]}-{s[6:8]}'
    return s if len(s) > 4 else None

def fmt_num(n):
    if n is None:
        return '暂无'
    if n >= 10000:
        return f'{n/10000:.1f}万'
    return f'{n:,.0f}'

# ── Region mapping ───────────────────────────────────────────
KNOWN_REGIONS = ['MAAS增长', '泛零售KA', '数字消费KA', '汽车酒旅KA', '政企金融KA', '海外']

def map_public_region(dept):
    """Extract region from CRM负责人部门 (e.g. '瓴羊/业务增长部/MAAS增长' → 'MAAS增长')."""
    if not dept:
        return '其他'
    last = dept.split('/')[-1].strip()
    if last in KNOWN_REGIONS:
        return last
    return '其他'

HYBRID_REGION_MAP = {
    '生态及SMB线': 'MAAS增长',
    '华南区': 'MAAS增长',
    '华东区': '泛零售KA',
    '华北区': '数字消费KA',
    '汽车能源交通行业线': '汽车酒旅KA',
    '政企金融行业线': '政企金融KA',
    '海外': '海外',
}

def map_hybrid_region(dept):
    """Map 客户负责人三级部门 to unified region."""
    if not dept:
        return '其他'
    dept = dept.strip()
    for k, v in HYBRID_REGION_MAP.items():
        if k in dept:
            return v
    return '其他'

# ── Hybrid cloud module definitions ──────────────────────────
# (display_name, status_col, price_col_or_None)
HYBRID_MODULES = [
    ('自助取数',       '自助取数',       '自助取数-钱'),
    ('移动微应用',     '移动微应用',     '移动微应用-钱'),
    ('监控告警',       '监控告警',       '监控告警-钱'),
    ('加速引擎',       '加速引擎',       '加速引擎-钱'),
    ('即席分析',       '即席分析',       '即席分析-钱'),
    ('数据填报',       '数据填报',       '数据填报-钱'),
    ('数据大屏',       '数据大屏',       '数据大屏-钱'),
    ('模版市场',       '模版市场',       '模板市场-钱'),
    ('电子表格',       '电子表格',       '电子表格-钱'),
    ('资源包',         '资源包',         '资源包-钱'),
    ('轻量ETL',        '数据准备-轻量ETL', '轻量ETL-钱'),
    ('交互式填报',     '交互式填报',     None),
    ('ticket增强嵌入', 'ticket增强嵌入', None),
    ('一键投产',       '一键投产',       None),
    ('智能问数',       '智能问数',       '智能问数-钱'),
    ('智能搭建',       '智能搭建',       '智能搭建-钱'),
    ('小Q报告',        '小Q报告',        '智能报告-钱'),
    ('归因洞察分析',   '归因洞察分析',   '波动归因-钱'),
]

# ══════════════════════════════════════════════════════════════
#   P A R T   A :   P U B L I C   C L O U D   P I P E L I N E
# ══════════════════════════════════════════════════════════════

# ── A1. Load public cloud customers (1047) ───────────────────
print('=' * 60)
print('PART A: Public Cloud Pipeline')
print('=' * 60)
print('Loading public cloud data...')
try:
    df_pub = pd.read_excel(F_PUBLIC, engine='calamine', skiprows=4)
except Exception:
    df_pub = pd.read_excel(F_PUBLIC, skiprows=4)

customers = {}
for _, row in df_pub.iterrows():
    name = safe_str(row.get('阿里云用户CID名称'))
    if not name:
        continue

    mpv = parse_module_pv(row.get('组织前1个月模块浏览PV信息'))
    total_pv = safe_num(row.get('组织前1月 PV数'), 0)
    module_count = len(mpv)
    dashboard_pv = mpv.get('仪表板', 0)
    dashboard_dom = round(dashboard_pv / total_pv, 2) if total_pv > 0 else 0

    dept = safe_str(row.get('CRM负责人部门'))

    customers[name] = {
        'name': name,
        'cid': safe_str(row.get('阿里云用户CID')),
        'uid': safe_str(row.get('UID')),
        'industry': safe_str(row.get('阿里云行业')),
        'industryDetail': '',
        'revenueEstimate': '',
        'priority': '',
        'priorityReason': '',
        'health': safe_str(row.get('健康度_新')),
        'activeUserRate': safe_pct(row.get('活跃用户占比')),
        'monthlyPV': int(total_pv),
        'version': safe_str(row.get('购买版本')),
        'accountCount': int(safe_num(row.get('实际账号数'), 0)),
        'accountActivation': safe_pct(row.get('账号开通率')),
        'orgMembers': int(safe_num(row.get('组织成员数量'), 0)),
        'reportCount': int(safe_num(row.get('报表数'), 0)),
        'renewalAmount': safe_num(row.get('预估续费金额')),
        'industryMax': None,
        'industryAvg': None,
        'gapToMax': None,
        'gapToAvg': None,
        'scores': {
            'usageDepth': 0,
            'upsellSpace': 0,
            'enterpriseScale': 0,
            'salesReach': 0,
            'composite': 0,
        },
        'modulePV': mpv,
        'moduleCount': module_count,
        'totalModulePV': int(total_pv),
        'dashboardDominance': dashboard_dom,
        'xiaoQ': {
            'status': None,
            'detail': None,
            'purchased': False,
            'trial': False,
            'trialMonths': None,
            'dailyTokens': None,
            'totalTokens': None,
            'purchaseDetail': None,
            'agents': {'report': 0, 'build': 0, 'query': 0},
        },
        'owner': safe_str(row.get('大客运营负责人')),
        'contact': safe_str(row.get('客户负责人')),
        'department': dept,
        'region': map_public_region(dept),
        'renewalStatus': safe_str(row.get('是否续约')),
        'customerTier': safe_str(row.get('客户分层(新)')),
        'customerSegment': safe_str(row.get('客户分层')),
        'expiryDate': fmt_date(row.get('到期时间(day)')),
        'activationDate': fmt_date(row.get('开通时间(day)')),
        'source': '',
    }

print(f'  Loaded {len(customers)} customers from public cloud file')

# ── A2. Enrich from legacy 261-customer file ─────────────────
print('Enriching from legacy 261 file...')
legacy_names = set()          # Track legacy customers for priority assignment
try:
    df_legacy = pd.read_excel(F_LEGACY, sheet_name='汇总(261家)')
    enriched = 0
    for _, row in df_legacy.iterrows():
        name = safe_str(row.get('阿里云用户CID名称'))
        if name not in customers:
            continue
        c = customers[name]
        legacy_names.add(name)
        c['scores'] = {
            'usageDepth': safe_num(row.get('使用深度得分'), 0),
            'upsellSpace': safe_num(row.get('增购空间得分'), 0),
            'enterpriseScale': safe_num(row.get('企业规模得分'), 0),
            'salesReach': safe_num(row.get('销售触达得分'), 0),
            'composite': safe_num(row.get('综合得分'), 0),
        }
        c['priority'] = '高优先级'    # Legacy customers are always high priority
        pr = safe_str(row.get('增购建议理由'))
        if pr:
            c['priorityReason'] = pr
        c['industryMax'] = safe_num(row.get('行业最高续费金额'))
        c['industryAvg'] = safe_num(row.get('行业平均续费金额'))
        c['gapToMax'] = safe_num(row.get('与行业最高差值'))
        c['gapToAvg'] = safe_num(row.get('与行业平均差值'))
        c['revenueEstimate'] = safe_str(row.get('预估年营收量级'))
        c['source'] = safe_str(row.get('来源分类'))
        xq_status = safe_str(row.get('智能小Q状态'))
        xq_detail = safe_str(row.get('智能小Q使用详情'))
        if xq_status:
            c['xiaoQ']['status'] = xq_status
            c['xiaoQ']['purchased'] = '已采购' in xq_status
        if xq_detail:
            c['xiaoQ']['detail'] = xq_detail
        enriched += 1
    print(f'  Enriched {enriched} customers from legacy file')
except Exception as e:
    print(f'  Warning: Could not load legacy file: {e}')

# ── A3. Enrich from 华东高潜力客户底池 (87 detail customers) ──
print('Enriching from detail file...')
try:
    df_detail = pd.read_excel(F_DETAIL)
    enriched = 0
    for _, row in df_detail.iterrows():
        name = safe_str(row.get('阿里云用户CID名称'))
        if name not in customers:
            continue
        c = customers[name]
        modules = ['仪表板', '数据门户', '电子表格', '数据填报', '数据大屏', '自助取数', '即席分析', 'QREPORT']
        for mod in modules:
            col = f'模块PV-{mod}'
            val = safe_num(row.get(col))
            if val is not None and val > 0:
                c['modulePV'][mod] = int(val)
        if c['modulePV']:
            c['moduleCount'] = len([v for v in c['modulePV'].values() if v > 0])
            total = sum(c['modulePV'].values())
            if total > 0:
                c['totalModulePV'] = total
                c['dashboardDominance'] = round(c['modulePV'].get('仪表板', 0) / total, 2)
        if safe_str(row.get('小Q-是否采购')) == '是':
            c['xiaoQ']['purchased'] = True
            agents = []
            for agent_name, col_name in [('报告', '小Q采购-报告Agent'), ('搭建', '小Q采购-搭建Agent'), ('问数', '小Q采购-问数Agent')]:
                v = safe_num(row.get(col_name), 0)
                if v > 0:
                    agents.append(f'{agent_name}x{int(v)}')
                    if '报告' in agent_name:
                        c['xiaoQ']['agents']['report'] = int(v)
                    elif '搭建' in agent_name:
                        c['xiaoQ']['agents']['build'] = int(v)
                    elif '问数' in agent_name:
                        c['xiaoQ']['agents']['query'] = int(v)
            c['xiaoQ']['purchaseDetail'] = ', '.join(agents) if agents else None
        if safe_str(row.get('小Q-是否试用')) == '是':
            c['xiaoQ']['trial'] = True
            c['xiaoQ']['trialMonths'] = safe_num(row.get('小Q试用月数'))
            c['xiaoQ']['totalTokens'] = safe_num(row.get('小Q总消耗Tokens'))
            c['xiaoQ']['dailyTokens'] = safe_num(row.get('小Q日均消耗Tokens'))
        enriched += 1
    print(f'  Enriched {enriched} customers')
except Exception as e:
    print(f'  Warning: Could not load detail file: {e}')

# ── A4. Enrich from industry classification ──────────────────
print('Enriching industry data...')
try:
    df_ind = pd.read_excel(F_INDUST)
    enriched = 0
    ind_map = {}
    for _, row in df_ind.iterrows():
        n = safe_str(row.get('客户名称'))
        if n:
            ind_map[n] = {
                'industryMajor': safe_str(row.get('行业大类')),
                'industrySub': safe_str(row.get('细分行业')),
            }
    for name, c in customers.items():
        if name in ind_map:
            c['industryDetail'] = ind_map[name].get('industrySub', '')
            if not c['industry']:
                c['industry'] = ind_map[name].get('industryMajor', '')
            enriched += 1
        else:
            for ind_name, ind_data in ind_map.items():
                if ind_name in name or name in ind_name:
                    c['industryDetail'] = ind_data.get('industrySub', '')
                    if not c['industry']:
                        c['industry'] = ind_data.get('industryMajor', '')
                    enriched += 1
                    break
    print(f'  Enriched {enriched} customers with industry data')
except Exception as e:
    print(f'  Warning: Could not load industry file: {e}')

# ── A5. Enrich from 小Q data ─────────────────────────────────
print('Enriching 小Q data...')
try:
    df_trial = pd.read_excel(F_XIAOQ, sheet_name='小q的token试用情况')
    trial_agg = {}
    for _, row in df_trial.iterrows():
        n = safe_str(row.get('cid_name'))
        if not n:
            continue
        tokens = safe_num(row.get('客户平均消耗Tokens'), 0)
        daily = safe_num(row.get('日均消耗Tokens'), 0)
        month = safe_str(row.get('distribution_time(month)'))
        if n not in trial_agg:
            trial_agg[n] = {'months': 0, 'total_tokens': 0, 'daily_tokens': 0, 'latest_month': ''}
        trial_agg[n]['months'] += 1
        trial_agg[n]['total_tokens'] += tokens
        if month > trial_agg[n]['latest_month']:
            trial_agg[n]['latest_month'] = month
            trial_agg[n]['daily_tokens'] = daily
    trial_enriched = 0
    for name, c in customers.items():
        if name in trial_agg:
            t = trial_agg[name]
            c['xiaoQ']['trial'] = True
            c['xiaoQ']['trialMonths'] = c['xiaoQ']['trialMonths'] or t['months']
            c['xiaoQ']['totalTokens'] = c['xiaoQ']['totalTokens'] or t['total_tokens']
            c['xiaoQ']['dailyTokens'] = c['xiaoQ']['dailyTokens'] or t['daily_tokens']
            trial_enriched += 1
    print(f'  Trial data enriched: {trial_enriched} customers')

    df_purchase = pd.read_excel(F_XIAOQ, sheet_name='小q的采购情况')
    if len(df_purchase) > 1:
        df_purchase = df_purchase.iloc[1:]
    purchase_enriched = 0
    for _, row in df_purchase.iterrows():
        n = safe_str(row.iloc[0]) if len(row) > 0 else ''
        if not n or n in ('cid_name', ''):
            continue
        for name, c in customers.items():
            if n.strip() == name.strip():
                c['xiaoQ']['purchased'] = True
                r_agent = safe_num(row.iloc[1] if len(row) > 1 else 0, 0)
                b_agent = safe_num(row.iloc[2] if len(row) > 2 else 0, 0)
                q_agent = safe_num(row.iloc[3] if len(row) > 3 else 0, 0)
                if r_agent > 0:
                    c['xiaoQ']['agents']['report'] = max(c['xiaoQ']['agents']['report'], int(r_agent))
                if b_agent > 0:
                    c['xiaoQ']['agents']['build'] = max(c['xiaoQ']['agents']['build'], int(b_agent))
                if q_agent > 0:
                    c['xiaoQ']['agents']['query'] = max(c['xiaoQ']['agents']['query'], int(q_agent))
                parts = []
                if c['xiaoQ']['agents']['report'] > 0:
                    parts.append(f"报告x{c['xiaoQ']['agents']['report']}")
                if c['xiaoQ']['agents']['build'] > 0:
                    parts.append(f"搭建x{c['xiaoQ']['agents']['build']}")
                if c['xiaoQ']['agents']['query'] > 0:
                    parts.append(f"问数x{c['xiaoQ']['agents']['query']}")
                if parts:
                    c['xiaoQ']['purchaseDetail'] = ', '.join(parts)
                purchase_enriched += 1
                break
    print(f'  Purchase data enriched: {purchase_enriched} customers')
except Exception as e:
    print(f'  Warning: Could not load 小Q data: {e}')

# ── A6. Enrich from 华东客户群 ───────────────────────────────
print('Enriching from 华东客户群...')
try:
    df_hd = pd.read_excel(F_HUADONG)
    enriched = 0
    for _, row in df_hd.iterrows():
        n = safe_str(row.get('客户名称'))
        if n in customers:
            c = customers[n]
            c['upsellPotential'] = safe_str(row.get('增购潜力'))
            amt = row.get('当前续费金额')
            if amt is not None and not pd.isna(amt):
                s = str(amt)
                if '万' in s:
                    try:
                        c['currentRenewal'] = float(s.replace('万', '')) * 10000
                    except:
                        pass
                else:
                    v = safe_num(amt)
                    if v is not None:
                        c['currentRenewal'] = v
            enriched += 1
    print(f'  Enriched {enriched} customers')
except Exception as e:
    print(f'  Warning: Could not load 华东客户群: {e}')

# ── A7. Classification (Plan A / B / C) ──────────────────────
print('Classifying customers...')

def classify(c):
    xq = c['xiaoQ']
    has_xq = (
        xq['purchased'] or
        xq['trial'] or
        (xq['totalTokens'] is not None and xq['totalTokens'] > 0) or
        (xq['status'] and ('采购' in xq['status'] or '增购潜力' in xq['status']))
    )
    if has_xq:
        return 'A', 'AI数字员工'
    mc = c['moduleCount']
    tp = c['totalModulePV']
    non_dash = {k: v for k, v in c['modulePV'].items() if k != '仪表板' and v > 0}
    has_deep = len(non_dash) >= 2 and sum(non_dash.values()) > 500
    if (mc >= 3 and tp >= 5000) or has_deep:
        return 'C', '深度场景加速'
    return 'B', 'AI基础提效'

plan_counts = {'A': 0, 'B': 0, 'C': 0}
for name, c in customers.items():
    plan, plan_name = classify(c)
    c['plan'] = plan
    c['planName'] = plan_name
    plan_counts[plan] += 1

print(f'  Plan A: {plan_counts["A"]}, Plan B: {plan_counts["B"]}, Plan C: {plan_counts["C"]}')

# ── A7b. Assign priority to non-legacy customers ─────────────
print('Assigning priority to non-legacy customers...')
priority_counts = {'高优先级': len(legacy_names), '中优先级': 0, '低优先级': 0}
for name, c in customers.items():
    if name in legacy_names:
        continue   # Already set to 高优先级 in A2
    is_healthy = c['health'] == '健康'
    has_pv = c['monthlyPV'] > 0
    is_plan_ac = c['plan'] in ('A', 'C')
    if (is_healthy and has_pv) or is_plan_ac:
        c['priority'] = '中优先级'
        priority_counts['中优先级'] += 1
    else:
        c['priority'] = '低优先级'
        priority_counts['低优先级'] += 1
print(f'  Priority distribution: {priority_counts}')

# ── A8. Generate talking points ──────────────────────────────
print('Generating talking points...')

def gen_talking_points(c):
    points = []
    plan = c['plan']
    xq = c['xiaoQ']

    if plan == 'A':
        reason = '贵司'
        if xq['purchased']:
            reason += f"已采购智能小Q（{xq['purchaseDetail'] or 'Agent'}）"
            if xq['trial'] and xq['dailyTokens']:
                reason += f"，并有试用数据（日均Token {fmt_num(xq['dailyTokens'])}）"
            reason += '，说明团队已建立AI分析习惯。建议补全Agent能力，形成完整AI工作流。'
        elif xq['trial']:
            reason += f"已试用智能小Q"
            if xq['trialMonths']:
                reason += f" {int(xq['trialMonths'])} 个月"
            if xq['dailyTokens']:
                reason += f"，日均Token消耗 {fmt_num(xq['dailyTokens'])}"
            reason += '，说明团队已形成AI分析习惯。正式采购 = 保护已建立的效率增量。'
        else:
            reason += '已接触智能小Q能力，建议进一步深化AI分析，提升团队整体数据能力。'
        c['planReason'] = reason
        if xq['trial'] and xq['dailyTokens']:
            points.append(f"贵司已试用小Q{'共'+str(int(xq['trialMonths']))+'个月' if xq['trialMonths'] else ''}，日均Token消耗{fmt_num(xq['dailyTokens'])}，说明团队已形成AI分析的工作习惯。试用额度有限，正式采购可保障服务连续性。")
        elif xq['purchased']:
            points.append(f"贵司已采购智能小Q（{xq['purchaseDetail'] or 'Agent'}），建议评估是否需要补充其他Agent以覆盖完整分析场景。")
        all_agents = {'report': '报告Agent', 'build': '搭建Agent', 'query': '问数Agent'}
        missing = [v for k, v in all_agents.items() if xq['agents'].get(k, 0) == 0]
        if missing and xq['purchased']:
            points.append(f"目前尚未开通{'、'.join(missing)}，补全后可实现从取数到报告的完整AI工作流，显著提升团队效率。")
        if c['monthlyPV'] > 0:
            points.append(f"贵司月PV达{fmt_num(c['monthlyPV'])}，{'活跃率'+str(c['activeUserRate'])+'%' if c['activeUserRate'] else ''}，通过小Q可将AI分析能力赋能至更多业务角色。")
        if c['renewalAmount'] and c['industryAvg'] and c['renewalAmount'] < c['industryAvg']:
            points.append(f"当前续费金额{fmt_num(c['renewalAmount'])}元，低于行业均值{fmt_num(c['industryAvg'])}元，通过AI提效增购ROI高。")
        points.append("建议从基础包（问数+解读Agent）起步验证，再逐步扩展到报告、发现、搭建Agent的完整AI工作流。")

    elif plan == 'B':
        dash_pv = c['modulePV'].get('仪表板', 0)
        reason = f"贵司目前以仪表板为主"
        if dash_pv > 0:
            reason += f"（月PV {fmt_num(dash_pv)}）"
        if c['reportCount'] > 0:
            reason += f"，已有{c['reportCount']}张报表"
        reason += '，数据基建扎实但尚未接触AI分析能力。引入智能小Q可零成本激活现有报表资产，让业务人员用自然语言直接获取洞察。'
        c['planReason'] = reason
        if dash_pv > 0 or c['reportCount'] > 0:
            parts = []
            if c['reportCount'] > 0:
                parts.append(f"已有{c['reportCount']}张报表")
            if dash_pv > 0:
                parts.append(f"仪表板月PV {fmt_num(dash_pv)}")
            points.append(f"贵司{('、').join(parts)}，数据资产基础扎实。但报表消费率普遍不足30%，AI解读可零成本激活这些沉睡资产。")
        points.append("智能小Q支持免费试用，零门槛启动——无需新建数据集，直接复用现有报表，开通即用，第一天就能看到效果。")
        if c['industry']:
            points.append(f"{c['industry']}行业头部客户已在部署AI分析能力，提前布局可建立数据竞争优势。")
        if c['orgMembers'] > 0 and c['activeUserRate'] is not None:
            points.append(f"贵司组织成员{c['orgMembers']}人，当前活跃率{c['activeUserRate']}%。通过小Q自然语言交互，可将数据分析能力从专业人员拓展至全员。")
        points.append("建议从小Q解读+发现起步（解读包），2周内验证报表消费率提升效果后再扩展到搭建和问数能力。")

    elif plan == 'C':
        active_mods = [f"{k}(PV:{fmt_num(v)})" for k, v in sorted(c['modulePV'].items(), key=lambda x: -x[1]) if v > 0]
        reason = f"贵司是QuickBI深度用户，活跃使用{c['moduleCount']}个模块"
        if active_mods:
            reason += f"（{', '.join(active_mods[:3])}等）"
        reason += f"，月总PV达{fmt_num(c['totalModulePV'])}。业务已深度依赖平台，但随着数据量和用户量增长，需要Quick引擎加速保障体验。"
        c['planReason'] = reason
        if active_mods:
            points.append(f"贵司深度使用{c['moduleCount']}个模块（{'、'.join([m.split('(')[0] for m in active_mods[:4]])}），月PV达{fmt_num(c['totalModulePV'])}，说明数据分析已深入业务核心流程。")
        spreadsheet_pv = c['modulePV'].get('电子表格', 0)
        if spreadsheet_pv > 1000:
            points.append(f"电子表格月PV {fmt_num(spreadsheet_pv)}，数据量大时查询易卡顿。Quick引擎抽取加速可实现60倍提速（30s>0.5s）。")
        portal_pv = c['modulePV'].get('数据门户', 0)
        fill_pv = c['modulePV'].get('数据填报', 0)
        if portal_pv > 0 or fill_pv > 0:
            parts = []
            if portal_pv > 0:
                parts.append(f"数据门户PV {fmt_num(portal_pv)}")
            if fill_pv > 0:
                parts.append(f"数据填报PV {fmt_num(fill_pv)}")
            points.append(f"贵司{'、'.join(parts)}，随着用户增长，页面加载和并发稳定性将成为关键。查询缓存+维值加速可保障秒级响应。")
        if c['renewalAmount'] and c['industryMax'] and c['gapToMax']:
            points.append(f"当前续费金额{fmt_num(c['renewalAmount'])}元，行业最高{fmt_num(c['industryMax'])}元，通过深化使用+引擎加速可释放更大价值空间。")
        points.append("建议从Quick引擎包起步解决性能瓶颈，再叠加报告Agent实现跨模块AI智能分析。")

    c['talkingPoints'] = points[:5]

for c in customers.values():
    gen_talking_points(c)

# ── A9. Build filters & output public.json ───────────────────
pub_filters = {
    'industries': sorted(set(c['industry'] for c in customers.values() if c['industry'])),
    'departments': sorted(set(c['department'] for c in customers.values() if c['department'])),
    'regions': sorted(set(c['region'] for c in customers.values())),
    'plans': ['A', 'B', 'C'],
    'priorities': sorted(set(c['priority'] for c in customers.values() if c['priority'])),
    'health': sorted(set(c['health'] for c in customers.values() if c['health'])),
}

pub_region_dist = {}
for c in customers.values():
    r = c['region']
    pub_region_dist[r] = pub_region_dist.get(r, 0) + 1

pub_output = {
    'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'totalCustomers': len(customers),
    'planDistribution': plan_counts,
    'regionDistribution': pub_region_dist,
    'filters': pub_filters,
    'customers': customers,
}

os.makedirs(DATA_DIR, exist_ok=True)
with open(OUT_PUBLIC, 'w', encoding='utf-8') as f:
    json.dump(pub_output, f, ensure_ascii=False, indent=2)

print(f'\n  Public cloud output: {OUT_PUBLIC}')
print(f'  Total customers: {len(customers)}')
print(f'  Region distribution: {pub_region_dist}')
print(f'  File size: {os.path.getsize(OUT_PUBLIC) / 1024:.1f} KB')


# ══════════════════════════════════════════════════════════════
#   P A R T   B :   H Y B R I D   C L O U D   P I P E L I N E
# ══════════════════════════════════════════════════════════════

print('\n' + '=' * 60)
print('PART B: Hybrid Cloud Pipeline')
print('=' * 60)
print('Loading hybrid cloud data...')

df_hyb = pd.read_csv(F_HYBRID)
hybrid_customers = {}

for _, row in df_hyb.iterrows():
    name = safe_str(row.get('客户名称'))
    if not name:
        continue

    dept = safe_str(row.get('客户负责人三级部门'))

    # Parse modules
    modules = {}
    for display_name, status_col, price_col in HYBRID_MODULES:
        status = safe_str(row.get(status_col))
        amount = safe_num(row.get(price_col), 0) if price_col else 0
        modules[display_name] = {
            'status': status,
            'amount': amount,
        }

    hybrid_customers[name] = {
        'name': name,
        'customerId': safe_str(row.get('客户ID')),
        'owner': safe_str(row.get('大客运营')),
        'contact': safe_str(row.get('客户负责人')),
        'department': dept,
        'region': map_hybrid_region(dept),
        'maintenanceStatus': safe_str(row.get('维保状态')),
        'priority': safe_str(row.get('优先级')),
        'isPublicPool': safe_str(row.get('是否公海')) == '是',
        'mainBIAmount': safe_num(row.get('主功能BI'), 0),
        'subscription': safe_str(row.get('是否订阅')),
        'version': safe_str(row.get('版本')),
        'deployment': safe_str(row.get('环境维表')),
        'authorization': safe_str(row.get('授权记录')),
        'highPotentialCount': int(safe_num(row.get('高潜模块个数'), 0)),
        'modules': modules,
    }

print(f'  Loaded {len(hybrid_customers)} customers from hybrid cloud file')

# Build hybrid filters & stats
hyb_region_dist = {}
for c in hybrid_customers.values():
    r = c['region']
    hyb_region_dist[r] = hyb_region_dist.get(r, 0) + 1

hyb_priority_dist = {}
for c in hybrid_customers.values():
    p = c['priority'] or '未标注'
    hyb_priority_dist[p] = hyb_priority_dist.get(p, 0) + 1

hyb_filters = {
    'regions': sorted(set(c['region'] for c in hybrid_customers.values())),
    'priorities': sorted(set(c['priority'] for c in hybrid_customers.values() if c['priority'])),
    'maintenanceStatus': sorted(set(c['maintenanceStatus'] for c in hybrid_customers.values() if c['maintenanceStatus'])),
}

hyb_output = {
    'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'totalCustomers': len(hybrid_customers),
    'regionDistribution': hyb_region_dist,
    'priorityDistribution': hyb_priority_dist,
    'filters': hyb_filters,
    'moduleNames': [m[0] for m in HYBRID_MODULES],
    'customers': hybrid_customers,
}

with open(OUT_HYBRID, 'w', encoding='utf-8') as f:
    json.dump(hyb_output, f, ensure_ascii=False, indent=2)

print(f'\n  Hybrid cloud output: {OUT_HYBRID}')
print(f'  Total customers: {len(hybrid_customers)}')
print(f'  Region distribution: {hyb_region_dist}')
print(f'  Priority distribution: {hyb_priority_dist}')
print(f'  File size: {os.path.getsize(OUT_HYBRID) / 1024:.1f} KB')


# ══════════════════════════════════════════════════════════════
#   P A R T   C :   M E T A   O U T P U T
# ══════════════════════════════════════════════════════════════

print('\n' + '=' * 60)
print('PART C: Meta Output')
print('=' * 60)

all_regions = sorted(set(
    list(pub_region_dist.keys()) + list(hyb_region_dist.keys())
))

meta = {
    'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'regions': all_regions,
    'publicCloud': {
        'totalCustomers': len(customers),
        'planDistribution': plan_counts,
        'regionDistribution': pub_region_dist,
    },
    'hybridCloud': {
        'totalCustomers': len(hybrid_customers),
        'regionDistribution': hyb_region_dist,
        'priorityDistribution': hyb_priority_dist,
        'moduleNames': [m[0] for m in HYBRID_MODULES],
    },
}

with open(OUT_META, 'w', encoding='utf-8') as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)

print(f'  Meta output: {OUT_META}')
print(f'  Unified regions: {all_regions}')
print(f'  File size: {os.path.getsize(OUT_META) / 1024:.1f} KB')

print('\n' + '=' * 60)
print('All done!')
print(f'  Public cloud:  {len(customers)} customers → {OUT_PUBLIC}')
print(f'  Hybrid cloud:  {len(hybrid_customers)} customers → {OUT_HYBRID}')
print(f'  Meta:          {OUT_META}')
print('=' * 60)
