import os
import time
import json
import logging
import yaml
import requests
import asyncio
import urllib3
import html
import pandas as pd
import numpy as np
import dataframe_image as dfi
from io import StringIO, BytesIO
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from telegram import Bot
from telegram.request import HTTPXRequest
from google.cloud import storage
import google.generativeai as genai

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== 0. 初始化与配置加载 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_path(filename):
    return os.path.join(BASE_DIR, filename)

def load_config():
    config_path = get_path("config.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

# 日志配置 (Cloud Run 会自动收集 stdout 日志)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("AutoReport")

IST_TZ = timezone(timedelta(hours=CONFIG['system']['timezone_offset']))

# 标准目标率配置
TARGET_VALS = {
    ('RM1(CashDaily)', 'First loan'): 0.50,
    ('RM1(CashDaily)', 'Reloan'): 0.60,
    ('RM1(QuickRupee)', 'First loan'): 0.50,
    ('RM1(QuickRupee)', 'Reloan'): 0.60,
    ('RM1(Other Apps)', 'First loan'): 0.50,
    ('RM1(Other Apps)', 'Reloan'): 0.60,
    ('RM0', 'First loan'): 0.15,
    ('RM0', 'Reloan'): 0.20,
    ('M1-1', 'First loan'): 0.02,
    ('M1-1', 'Reloan'): 0.02
}

# ==================== 1. API 数据解析工具 ====================

def clean_and_parse_data(content_bytes):
    if not content_bytes: return pd.DataFrame()
    df_raw = None
    try:
        df_raw = pd.read_excel(BytesIO(content_bytes), engine='openpyxl')
    except:
        try:
            text_data = content_bytes.decode('utf-8', errors='ignore').split('{"info":')[0]
            for sep in ['\t', ',']:
                temp_df = pd.read_csv(StringIO(text_data), sep=sep, on_bad_lines='skip')
                if temp_df.shape[1] >= 10: 
                    df_raw = temp_df
                    break
        except: pass

    if df_raw is None or df_raw.empty: return pd.DataFrame()

    # --- 智能识别新旧两种格式 ---
    
    # 检查是否为新版历史数据格式 (包含特定的列名)
    if 'Ticket Category' in df_raw.columns and 'Date' in df_raw.columns:
        # 定义新列名到标准内部列名的映射
        col_map = {
            'Date': 'TimePoint',
            'Ticket Category': 'Stage',
            'Is Reloan': 'LoanType',
            'Assign To': 'AssignTo',
            'Total Left Unpaid Principal': 'TotalLeft',
            'Total Repay Amount': 'TotalRepayAmount',
            'Repay Rate': 'RepayRate',
            'Load Num': 'LoadNum',
            'role': 'role',
            'App': 'APP',
            'APP': 'APP'
        }
        df = df_raw.rename(columns=col_map).copy()
        
        # 兜底：处理可能的各种App大小写
        for c in df.columns:
            if c.lower() == 'app' and c != 'APP':
                df.rename(columns={c: 'APP'}, inplace=True)
                
        # 确保关键列存在，防止报错
        for col in ['TotalLeft', 'TotalRepayAmount', 'TimePoint', 'Stage', 'LoanType', 'AssignTo', 'APP']:
            if col not in df.columns:
                # 数值列补0，字符串列补空
                if 'Total' in col: df[col] = 0
                else: df[col] = ''
    else:
        # 旧版逻辑: 按索引截取前20列
        df = df_raw.iloc[:, 0:20].copy()
        df.columns = [
            'id','EmployeeID','TimePoint','Stage','LoanType','role','Ranking','AssignTo',
            'TotalLeft','RepayPrincipal','RepayInterest','RepayServiceFee',
            'TotalRepayAmount','RepayRate','TargetRepayRate','NewAssignNum',
            'HandleNum','CompleteNum','LoadNum','APP'
        ]
    
    # 清理 APP 名称中的 (在架) 等多余字符
    if 'APP' in df.columns:
        df['APP'] = df['APP'].astype(str).str.replace(r'\(在架\)', '', regex=True).str.strip()

    # 统一处理 Team 字段
    def extract_team(assign_to):
        name = str(assign_to).strip()
        if not name or name.lower() == 'nan':
            return 'Unknown'
        first_char = name[0].upper()
        if first_char == 'K':
            return name[:3].upper()
        else:
            return first_char

    if 'AssignTo' in df.columns:
        df['team'] = df['AssignTo'].apply(extract_team)
    else:
        df['team'] = 'Unknown'
        
    # --- 新增: 根据 APP 重塑 Stage (RM1拆分为CashDaily、QuickRupee和Other Apps) ---
    def remap_stage(row):
        stage = str(row.get('Stage', '')).strip().upper()
        # 标准化异常的 stage 命名
        norm_map = {'M1': 'RM1', 'M0': 'RM0', 'M1-1': 'M1-1', 'D0': 'RM0'}
        stage = norm_map.get(stage, stage)
        
        if 'RM1' in stage:
            app = str(row.get('APP', '')).strip().lower()
            if 'cashdaily' in app:
                return 'RM1(CashDaily)'
            elif 'quickrupee' in app:
                return 'RM1(QuickRupee)'
            else:
                return 'RM1(Other Apps)'
        else:
            return stage

    if 'Stage' in df.columns:
        df['Stage'] = df.apply(remap_stage, axis=1)

    return df

def fetch_raw_api_data(api_conf, time_range, limit="100000", custom_url=None, time_param_name="ctime_range"):
    # 允许覆盖 URL 和时间参数名
    url = custom_url if custom_url else api_conf['base_url']
    params = {time_param_name: time_range, "key": api_conf['key'], "export_type": "excel", "p": "1", "limit": limit}
    headers = {'User-Agent': 'Mozilla/5.0', 'Cookie': api_conf['cookie']}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=180, verify=False)
        return clean_and_parse_data(resp.content)
    except Exception as e:
        logger.error(f"API 获取失败: {e}")
        return None

# ==================== 2. 历史分析模块 ====================

class HistoricalAnalyzer:
    def __init__(self, api_conf): 
        self.api_conf = api_conf

    def load_and_calculate(self):
        now_ist = datetime.now(IST_TZ)
        # 计算 T-3 到 T-1 的日期 (格式 YYYY-MM-DD)
        start_date = (now_ist - timedelta(days=3)).strftime('%Y-%m-%d')
        end_date = (now_ist - timedelta(days=1)).strftime('%Y-%m-%d')
        range_str = f"{start_date} - {end_date}"
        
        # 指定历史数据 URL
        history_url = "https://dc.tidbi-it.cc/api/performance/worker/daily_export"
        
        logger.info(f"正在获取历史数据 (T-3 至 T-1): {range_str} [URL: {history_url}]")
        
        # 使用新的 URL 和参数名 date_range
        df_raw = fetch_raw_api_data(
            self.api_conf, 
            range_str, 
            limit="300000", 
            custom_url=history_url, 
            time_param_name="date_range"
        )
        
        if df_raw is None or df_raw.empty:
            logger.warning("历史数据为空")
            return {}
        
        # 数据清洗与类型转换
        # TimePoint 在新接口中是 Date (YYYYMMDD)
        df_raw['TP_val'] = pd.to_numeric(df_raw['TimePoint'], errors='coerce').fillna(0).astype(np.int64)
        
        # 确保只使用有效数据 (非0日期)
        df_finals = df_raw[df_raw['TP_val'] > 0].copy()
        
        result_map = {}
        for (stage, ltype), group in df_finals.groupby(['Stage', 'LoanType']):
            for c in ['TotalLeft', 'TotalRepayAmount']: 
                group[c] = pd.to_numeric(group[c], errors='coerce').fillna(0)
            
            cat_total_repay = group['TotalRepayAmount'].sum()
            cat_total_left = group['TotalLeft'].sum()
            # 计算该 Category 过去几天的整体平均回款率
            cat_3d_avg = (cat_total_repay / cat_total_left * 100) if cat_total_left > 0 else 0
            
            p_stats = group.groupby('AssignTo')[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
            p_stats['r'] = (p_stats['TotalRepayAmount'] / p_stats['TotalLeft'] * 100).fillna(0)
            p_stats['pct'] = p_stats['r'].rank(pct=True, method='min', ascending=True)
            
            for _, row in p_stats.iterrows():
                result_map[(str(stage).strip(), str(ltype).strip(), str(row['AssignTo']).strip())] = {
                    'is_lower_avg': row['r'] < cat_3d_avg, 
                    'is_bottom_20': row['pct'] <= 0.20
                }
        
        logger.info(f"历史数据分析完成，生成 {len(result_map)} 条人员画像")
        return result_map

# ==================== 3. 实时分析模块 (含 GCS 读写) ====================

class DataAnalyzer:
    def __init__(self, history_filename, historical_stats=None):
        self.history_file = history_filename
        self.bucket_name = CONFIG['system'].get('history_bucket')
        self.historical_stats = historical_stats or {}
        self.lagging_threshold = CONFIG['system'].get('lagging_threshold', 0.9)
        self.stagnation_hours = CONFIG['system'].get('stagnation_hours', 2)
        self.current_snapshot = {}
        self.history_list = self._load_history()

    def _load_history(self):
        """从 GCS 加载历史记录"""
        if not self.bucket_name:
            logger.warning("未配置 history_bucket，无法持久化记录")
            return []
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(self.history_file)
            if blob.exists():
                data = json.loads(blob.download_as_text())
                today = datetime.now(IST_TZ).strftime('%Y-%m-%d')
                return [h for h in data if datetime.fromtimestamp(h["timestamp"], IST_TZ).strftime('%Y-%m-%d') == today]
        except Exception as e:
            logger.error(f"从 GCS 加载历史失败: {e}")
        return []

    def save_history(self):
        """保存历史记录到 GCS"""
        if not self.current_snapshot or not self.bucket_name: return
        
        self.history_list.append({"timestamp": time.time(), "data": self.current_snapshot})
        today = datetime.now(IST_TZ).strftime('%Y-%m-%d')
        # 仅保留当天数据
        self.history_list = [h for h in self.history_list if datetime.fromtimestamp(h["timestamp"], IST_TZ).strftime('%Y-%m-%d') == today]
        
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(self.history_file)
            blob.upload_from_string(json.dumps(self.history_list, ensure_ascii=False, indent=2), content_type='application/json')
            logger.info("历史记录已保存至 GCS")
        except Exception as e:
            logger.error(f"保存历史到 GCS 失败: {e}", exc_info=True)

    def get_last_run_data(self, key):
        return self.history_list[-1]["data"].get(key) if self.history_list else None

    def get_2h_ago(self, key):
        if not self.history_list: return None
        target = time.time() - (self.stagnation_hours * 3600)
        best, min_diff = None, 5400 # 1.5小时误差内
        for r in self.history_list:
            diff = abs(r["timestamp"] - target)
            if diff < min_diff: min_diff = diff; best = r
        return best["data"].get(key) if best else None

    def process_team_data(self, df, team_id, team_name, admin_contact='@admin'):
        target_id = str(team_id).strip().upper()
        df_team = df[df['team'].astype(str).str.strip().str.upper() == target_id].copy()
        if df_team.empty: return None

        for c in ['TotalLeft', 'TotalRepayAmount', 'LoadNum']:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            df_team[c] = pd.to_numeric(df_team[c], errors='coerce').fillna(0)

        max_tp = pd.to_numeric(df['TimePoint'], errors='coerce').max()
        df_curr_all = df[pd.to_numeric(df['TimePoint'], errors='coerce') == max_tp].copy()
        
        g_avgs, g_p_ranks, g_team_ranks = {}, {}, {}
        for (st, lt), g in df_curr_all.groupby(['Stage', 'LoanType']):
            g_avgs[(st, lt)] = (g['TotalRepayAmount'].sum() / g['TotalLeft'].sum() * 100) if g['TotalLeft'].sum() > 0 else 0
            ps = g.groupby('AssignTo')[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
            ps['R'] = (ps['TotalRepayAmount'] / ps['TotalLeft'] * 100).fillna(0)
            ps = ps.sort_values('R', ascending=False).reset_index(drop=True)
            for i, r in ps.iterrows(): g_p_ranks[(st, lt, str(r['AssignTo']))] = f"{i+1}/{len(ps)}"
            ts = g.groupby('team')[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
            ts['R'] = (ts['TotalRepayAmount'] / ts['TotalLeft'] * 100).fillna(0)
            ts = ts.sort_values('R', ascending=False).reset_index(drop=True)
            for i, r in ts.iterrows(): g_team_ranks[(st, lt, str(r['team']))] = f"{i+1}/{len(ts)}"

        df_l = df_team[pd.to_numeric(df_team['TimePoint'], errors='coerce') == max_tp].copy()
        ist_now = datetime.now(IST_TZ).strftime("%Y%m%d %H:%M")
        
        safe_team_name = html.escape(team_name)
        common_header = f"<b>{safe_team_name} Hourly Collection Ranking - {ist_now} (IST)</b>\n"
        common_header += "❌️ 绩效低于实时均值 / Lagging\n"
        common_header += f"❌️🛑({self.stagnation_hours}h无催回) 绩效差且停滞/ Lagging&Stagnation\n"
        common_header += "⬆️ 代表本点催回上升 / Increased\n"
        common_header += "🚫 3d Re% lower 3d Avg | ⛔ 3d bottom 20%\n"
        common_header += "-"*45 + "\n"

        results = {}
        # For K- groups, combine all stages into a single report. For others, keep them separate.
        if target_id.startswith('K-'):
            stage_prefixes_list = [['RM1', 'RM0', 'M1-1']]
        else:
            stage_prefixes_list = [['RM1'], ['RM0'], ['M1-1']]

        for prefixes in stage_prefixes_list:
            stage_name_key = prefixes[0] if len(prefixes) == 1 else 'All Stages'
            stage_text, stage_plot_data = common_header, []
            
            sub_groups = []
            grouped_list = list(df_l.groupby(['Stage', 'LoanType']))
            for pfx in prefixes:
                sub_groups.extend([g for g in grouped_list if pfx in g[0][0]])
                
            if not sub_groups: continue

            for (st, lt), group in sub_groups:
                target_val = TARGET_VALS.get((st, lt), 1.0)
                target_pct = target_val * 100
                cat_avg = g_avgs.get((st, lt), 0)
                
                t_repay, t_left = group['TotalRepayAmount'].sum(), group['TotalLeft'].sum()
                t_rate = (t_repay / t_left * 100) if t_left > 0 else 0
                t_rank = g_team_ranks.get((st, lt, target_id), "-")
                t_achv = (t_rate / target_pct * 100) if target_pct > 0 else 0
                
                snap_k = f"{team_id}_{st}_{lt}_GRP"
                last_g = self.get_last_run_data(snap_k)
                g_diff = t_rate - (last_g.get('rate', 0) if last_g else 0)
                trend_icon = f"(⬆️ {abs(g_diff):.1f}%)" if g_diff >= 0.01 else (f"(⬇️ {abs(g_diff):.1f}%)" if g_diff <= -0.01 else f"(- {abs(g_diff):.1f}%)")
                self.current_snapshot[snap_k] = {'rate': float(t_rate), 'repay': float(t_repay)}

                stage_plot_data.append({
                    "Stage": st, "Type": lt, "Name": team_name, "Re%": f"{t_rate:.1f}%",
                    "Target": f"{target_pct:.1f}%", "Achv%": f"{t_achv:.0f}%",
                    "Diff.Avg": "-", "Rank": t_rank, "Tickets": int(group['LoadNum'].sum()),
                    "IsBold": True, "RateNum": t_rate
                })

                stage_text += f"                           <b>{st} {lt}</b>\n"
                stage_text += f"Team Rate: <b>{t_rate:.1f}%</b> {trend_icon} (Rank: <b>{t_rank}</b>)\n"
                stage_text += f"Target: <b>{target_pct:.1f}%</b> | Achv: <b>{t_achv:.0f}%</b>\n\n"
                
                stagnant_l, lagging_l, normal_l = [], [], []
                for _, row in group.groupby('AssignTo')[['TotalLeft', 'TotalRepayAmount', 'LoadNum']].sum().reset_index().iterrows():
                    name, p_repay, p_left = str(row['AssignTo']).strip(), row['TotalRepayAmount'], row['TotalLeft']
                    p_rate = (p_repay / p_left * 100) if p_left > 0 else 0
                    p_key = f"{team_id}_{st}_{lt}_{name}"
                    
                    last_p = self.get_last_run_data(p_key)
                    p_diff = p_rate - (last_p.get('rate', 0) if last_p else 0)
                    self.current_snapshot[p_key] = {'rate': float(p_rate), 'repay': float(p_repay)}
                    
                    hist = self.historical_stats.get((st, lt, name), {})
                    h_flags = ("⛔" if hist.get('is_bottom_20') else "") + ("🚫" if hist.get('is_lower_avg') else "")
                    
                    p_rank = g_p_ranks.get((st, lt, name), "-")
                    is_lag = (p_rate < cat_avg * self.lagging_threshold) or (p_rate == 0 and p_left > 0)
                    
                    # 1. 计算增长文本 (显示在行尾)
                    trend_text = f" ⬆️ {p_diff:.1f}%" if p_diff >= 0.01 else ""
                    
                    status_icon = ""
                    list_type = "normal"

                    if is_lag:
                        d2h = self.get_2h_ago(p_key)
                        if d2h and abs(p_repay - d2h.get('repay', 0)) < 1:
                            list_type = "stagnant"
                            status_icon = "❌️🛑"
                        else:
                            list_type = "lagging"
                            status_icon = "❌️"
                    
                    # 2. 构建信息对象，后缀包含所有状态和趋势
                    full_suffix = f"{h_flags}{status_icon}{trend_text}"
                    
                    info = {
                        'name': html.escape(name),
                        'rate': p_rate,
                        'rank': p_rank,
                        'suffix': full_suffix
                    }
                    
                    if list_type == "stagnant": stagnant_l.append(info)
                    elif list_type == "lagging": lagging_l.append(info)
                    else: normal_l.append(info)

                    # 3. 表格数据 (Trends 列显示简化图标)
                    df_trend_icon = f"{h_flags}{status_icon}"
                    if p_diff >= 0.01: df_trend_icon += "⬆️"
                    elif not df_trend_icon: df_trend_icon = "-"

                    stage_plot_data.append({
                        "Stage": st, "Type": lt, "Name": name, "Re%": f"{p_rate:.1f}%",
                        "Target": f"{target_pct:.1f}%", "Achv%": f"{((p_rate/target_pct*100)):.0f}%",
                        "Diff.Avg": f"{(p_rate-cat_avg):+.1f}%", "Rank": p_rank, "Tickets": int(row['LoadNum']),
                        "IsBold": False, "RateNum": p_rate
                    })

                if stagnant_l:
                    stage_text += "<b>绩效差且无进展 (2h无催回)</b>\n"
                    # 格式: Name : Rate (Rank) Flags
                    stage_text += "\n".join([f"{i['name']} : {i['rate']:.1f}% ({i['rank']}){i['suffix']}" for i in sorted(stagnant_l, key=lambda x: x['rate'])]) + "\n\n"
                if lagging_l:
                    stage_text += "<b>绩效差员工 (Lagging)</b>\n"
                    # 格式: Name : Rate (Rank) Flags
                    stage_text += "\n".join([f"{i['name']} : {i['rate']:.1f}% ({i['rank']}){i['suffix']}" for i in sorted(lagging_l, key=lambda x: x['rate'])]) + "\n\n"
                if normal_l:
                    stage_text += "<b>绩效正常员工 (Normal)</b>\n"
                    # 统一使用 • 前缀
                    stage_text += "\n".join([f"• {i['name']} : <b>{i['rate']:.1f}%</b> ({i['rank']}){i['suffix']}" for i in sorted(normal_l, key=lambda x: x['rate'], reverse=True)]) + "\n"
                stage_text += "-"*45 + "\n\n"

            if admin_contact and admin_contact != '@admin':
                stage_text += f"{html.escape(admin_contact)}\n\n"

            results[stage_name_key] = {'text': stage_text, 'df': pd.DataFrame(stage_plot_data)}
        return results

# ==================== 4. 绘图模块 ====================

def generate_image(df, team_id, stage_suffix, team_name):
    if df.empty: return None
    fn = f"/tmp/report_{team_id}_{stage_suffix}.png" # 使用 /tmp 目录
    ist_now_str = datetime.now(IST_TZ).strftime("%Y%m%d %H:%M")
    title_text = f"{team_name} {stage_suffix} Hourly Report - {ist_now_str} (IST)"

    def style_row(row):
        st, lt, is_bold = str(row['Stage']), str(row['Type']), row['IsBold']
        if st == 'RM0': bg = '#e6f7ff' if 'First' in lt else '#f9f0ff'
        else: bg = '#fffbe6' if 'First' in lt else '#f6ffed'
        styles = [f'background-color: {bg}'] * len(row)
        if is_bold: styles = [f'background-color: {bg}; font-weight: bold; color: #c0392b'] * len(row)
        return styles

    try:
        styler = df.style.set_caption(title_text) \
            .apply(style_row, axis=1) \
            .background_gradient(subset=['RateNum'], cmap='RdYlGn', vmin=0, vmax=60) \
            .hide(axis='index') \
            .hide(subset=['IsBold', 'RateNum'], axis='columns') \
            .set_properties(**{'border': '1px solid gray', 'text-align': 'center', 'font-size': '11pt', 'padding': '2px 5px'}) \
            .set_table_styles([
                {'selector': 'th', 'props': [('background-color', '#2c3e50'), ('color', 'white'), ('font-weight', 'bold')]},
                {'selector': 'caption', 'props': [('caption-side', 'top'), ('font-size', '14pt'), ('font-weight', 'bold'), ('padding', '10px'), ('color', '#2c3e50')]}
            ])
        # 显式指定 playwright 转换
        dfi.export(styler, fn, max_rows=150, table_conversion='playwright')
        return fn
    except Exception as e:
        logger.error(f"绘图失败: {e}"); return None

# ==================== 6. Nudge 模块 ====================
def generate_nudge_msg(team_name, admin_contact, bad_staff_df):
    """
    使用 Gemini 生成催促文案
    """
    # 获取配置中的 Prompt 设定，如果没有则使用默认
    prompt_config = CONFIG.get('nudge', {})
    model_name = prompt_config.get('gemini_model', 'gemini-1.5-flash')
    tone = prompt_config.get('tone', 'strict')
    
    # 优先从 config 读取 Key，其次从环境变量读取
    api_key = prompt_config.get('gemini_api_key') or os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key or "YOUR_GEMINI_API_KEY" in api_key:
        logger.error("未配置 GEMINI_API_KEY，无法生成 Nudge 文案")
        return None
        
    genai.configure(api_key=api_key)
    
    staff_list_str = ""
    if bad_staff_df is not None and not bad_staff_df.empty:
        for _, row in bad_staff_df.iterrows():
            staff_list_str += f"- {row['name']} : 进度 {row['rate']:.1f}% (排名: {row['rank']})\n"
    
    if tone == 'casual':
        system_prompt = f"""
        你是一个催收团队的严厉督导。
        你现在要对 {team_name} 组进行突击检查。
        管理员是 {admin_contact}。
        以下员工表现严重落后（低于平均值太多或产出为0）：
        {staff_list_str}
        
        请生成一条发到 Telegram 群的消息：
        1. 第一行必须 @管理员 ({admin_contact})。
        2. 列出上述差生名单 (格式: 名字 进度 排名)。
        3. 语气要紧迫、严厉、口语化、江湖气（例如："哥们尽快催"、"别掉链子"、"数据太难看了"、"再不跑起来要凉了"）。
        4. 只有100字左右，不要废话。
        5. 结尾加几个警示的 emoji (🛑, ⚠️, 😤 等)。
        """
    elif tone == 'normal':
        system_prompt = f"""
        你是一个催收团队的严厉督导。
        你现在要对 {team_name} 组进行突击检查。
        管理员是 {admin_contact}。
        以下员工表现严重落后（低于平均值太多或产出为0）：
        {staff_list_str}
        
        请生成一条发到 Telegram 群的消息：
        1. 第一行必须 @管理员 ({admin_contact})。
        2. 列出上述差生名单 (格式: 名字 进度 排名)。
        3. 语气要客观、职业、就事论事。不要像"strict"那样压迫，也不要像"casual"那样江湖气。
        4. 重点强调数据落后，要求管理员关注并督促改进。
        5. 只有100字左右，不要废话。
        6. 结尾加几个警示的 emoji (⚠️, 📉 等)。
        """
    else: # strict
        system_prompt = f"""
        你是一个催收团队的严厉督导。
        你现在要对 {team_name} 组进行突击检查。
        管理员是 {admin_contact}。
        以下员工表现严重落后（低于平均值太多或产出为0）：
        {staff_list_str}
        
        请生成一条发到 Telegram 群的消息：
        1. 第一行必须 @管理员 ({admin_contact})。
        2. 列出上述差生名单 (格式: 名字 进度 排名)。
        3. 语气要紧迫、严厉、专业但带有强烈的压迫感。直接指出问题，要求立即改进。
        4. 只有100字左右，不要废话。
        5. 结尾加几个警示的 emoji (🛑, ⚠️, 😤 等)。
        """
    
    try:
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(system_prompt)
        return response.text
    except Exception as e:
        logger.error(f"Gemini 调用失败: {e}")
        return None

# ==================== 5. 核心运行逻辑 (被 main.py 调用) ====================

async def run_cycle(run_mode='regular'):
    logger.info(f">>> 启动报表生成流程 (Mode: {run_mode})")
    
    # 获取历史 3 天统计
    h_analyzer = HistoricalAnalyzer(CONFIG['api'])
    h_stats = await asyncio.to_thread(h_analyzer.load_and_calculate)
    
    # 获取今日 API 数据 (使用默认的小时数据配置)
    now_ist = datetime.now(IST_TZ)
    time_range = f"{now_ist.strftime('%Y-%m-%d 06:00:00')} - {now_ist.strftime('%Y-%m-%d 23:59:59')}"
    df = await asyncio.to_thread(fetch_raw_api_data, CONFIG['api'], time_range)
    
    if df is None or df.empty:
        logger.warning("API 未返回有效数据，流程中止")
        return "No Data from API"

    analyzer = DataAnalyzer(CONFIG['system']['history_file'], historical_stats=h_stats)
    
    nudge_triggered_teams = []
    
    # 逐个团队处理
    for team_conf in CONFIG['teams']:
        bot = Bot(token=team_conf['bot_token'], request=HTTPXRequest())
        
        admin_contact = team_conf.get('admin', '@admin')
        
        # 注意: process_team_data 返回的是一个为了画图准备的数据结构
        # 为了 Nudge 模式，我们需要更直接的访问数据，或者复用 process_team_data 的结果
        report_data = analyzer.process_team_data(df, team_conf['id'], team_conf['name'], admin_contact)
        
        if not report_data: continue
        
        # === Nudge Mode ===
        if run_mode == 'nudge':
            # 只检查，不画图，不发完整报表
            # 我们需要遍历 report_data 里的数据来找到差生
            bad_staff_list = []
            
            for stage_name, data in report_data.items():
                # data['df'] 是绘图用的 DataFrame
                df_p = data['df']
                if df_p.empty: continue
                
                threshold = CONFIG.get('nudge', {}).get('threshold_ratio', 0.6)
                
                # df_p 可能包含多个 Type (如 First loan 和 Reloan)，必须按 Stage 和 Type 分组分别计算均值
                for (st, lt), group_df in df_p.groupby(['Stage', 'Type']):
                    team_row = group_df[group_df['IsBold'] == True]
                    if team_row.empty: continue
                    
                    team_rate = team_row.iloc[0]['RateNum']
                    staff_rows = group_df[group_df['IsBold'] == False]
                    
                    for _, row in staff_rows.iterrows():
                         rate = row['RateNum']
                         # 排除掉 rate 为 0 且 Tickets 为 0 的 (也就是没活干的人，不怪他)
                         if rate == 0 and row['Tickets'] == 0:
                             continue
    
                         if (rate < team_rate * threshold) or (rate == 0):
                             bad_staff_list.append({
                                 'name': f"{row['Name']} ({row['Stage']} {row['Type']})",
                                 'rate': rate,
                                 'rank': row['Rank']
                             })
            
            if bad_staff_list:
                bad_staff_df = pd.DataFrame(bad_staff_list)
                admin_contact = team_conf.get('admin', '@admin')
                
                nudge_msg = await asyncio.to_thread(generate_nudge_msg, team_conf['name'], admin_contact, bad_staff_df)
                
                if nudge_msg:
                    # 修复: LLM 经常会对用户名中的下划线进行 Markdown 转义，导致 Telegram 无法识别 @
                    nudge_msg = nudge_msg.replace(r"\_", "_").replace(r"\@", "@")
                    
                    try:
                        await bot.send_message(chat_id=team_conf['chat_id'], text=nudge_msg)
                        logger.info(f"Nudge 推送成功: {team_conf['name']}")
                        nudge_triggered_teams.append(team_conf['name'])
                    except Exception as e:
                        logger.error(f"Nudge 推送失败 ({team_conf['name']}): {e}")
            continue # Nudge 模式下，处理完一个队就继续下一个，跳过后面的常规逻辑

        # === Regular Mode ===
        for stage_name, data in report_data.items():
            txt, df_p = data['text'], data['df']
            img_path = await asyncio.to_thread(generate_image, df_p, team_conf['id'], stage_name, team_conf['name'])
            
            try:
                if img_path and os.path.exists(img_path):
                    with open(img_path, 'rb') as f:
                        await bot.send_photo(chat_id=team_conf['chat_id'], photo=f, caption=f"📊 {team_conf['name']} {stage_name} 实时报表")
                    os.remove(img_path)
                
                await bot.send_message(chat_id=team_conf['chat_id'], text=txt, parse_mode='HTML')
                logger.info(f"推送成功: {team_conf['name']} {stage_name}")
            except Exception as e:
                logger.error(f"推送失败 ({team_conf['name']} {stage_name}): {e}")
                
    if run_mode == 'regular':
        analyzer.save_history() # 只有常规模式才保存历史
        
    logger.info("<<< 流程执行完毕")
    
    if run_mode == 'nudge':
        if nudge_triggered_teams:
            return f"Nudge sent to: {', '.join(nudge_triggered_teams)}"
        else:
            return "Everyone is doing okay."
    return "Regular Report Cycle Completed"
