import os
import time
import json
import logging
import yaml
import requests
import asyncio
import urllib3
import pandas as pd
import dataframe_image as dfi
from io import BytesIO
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from telegram import Bot
from telegram.request import HTTPXRequest
from google.cloud import storage

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== 0. 初始化与配置加载 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_path(filename):
    return os.path.join(BASE_DIR, filename)

def load_config():
    config_path = get_path("config.yaml")
    if not os.path.exists(config_path):
        config_path = "config.yaml"
    
    if not os.path.exists(config_path):
        print(f"❌ [DEBUG] 找不到配置文件: {config_path}", flush=True)
        raise FileNotFoundError(f"未找到配置文件: {config_path}")
        
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

print("✅ [DEBUG] 正在加载配置文件...", flush=True)
CONFIG = load_config()
print("✅ [DEBUG] 配置文件加载成功", flush=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AutoReport")

IST_TZ = timezone(timedelta(hours=CONFIG['system']['timezone_offset']))

# ==================== 1. 标准催回率参考表 ====================
TARGET_RATES = {
    ('RM1', 'First loan'): {7: 0.002, 8: 0.010, 9: 0.047, 10: 0.106, 11: 0.189, 12: 0.271, 13: 0.356, 14: 0.380, 15: 0.401, 16: 0.414, 17: 0.425, 18: 0.434, 19: 0.446, 20: 0.453, 21: 0.455, 22: 0.457, 23: 0.459},
    ('RM1', 'Reloan'): {7: 0.001, 8: 0.026, 9: 0.060, 10: 0.116, 11: 0.236, 12: 0.332, 13: 0.440, 14: 0.474, 15: 0.532, 16: 0.555, 17: 0.578, 18: 0.603, 19: 0.608, 20: 0.615, 21: 0.615, 22: 0.615, 23: 0.620},
    ('RM0', 'First loan'): {7: 0.000, 8: 0.003, 9: 0.009, 10: 0.026, 11: 0.043, 12: 0.058, 13: 0.072, 14: 0.083, 15: 0.102, 16: 0.107, 17: 0.124, 18: 0.124, 19: 0.124, 20: 0.127, 21: 0.130, 22: 0.133, 23: 0.133},
    ('RM0', 'Reloan'): {7: 0.005, 8: 0.035, 9: 0.035, 10: 0.041, 11: 0.055, 12: 0.078, 13: 0.093, 14: 0.107, 15: 0.113, 16: 0.113, 17: 0.122, 18: 0.137, 19: 0.144, 20: 0.151, 21: 0.156, 22: 0.156, 23: 0.156}
}

class APIClient:
    def __init__(self, api_conf):
        self.conf = api_conf
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Cookie': self.conf['cookie'],
            'Referer': 'https://dc.tidbi-it.cc/'
        }

    def process_team_assignment(self, df):
        # 依据 AssignTo(数据源H列) 首字母分配 Team，如果 K 开头则取前 3 个字符
        if 'AssignTo' in df.columns:
            def get_team(n):
                n = str(n).strip().upper()
                if not n: return 'Unknown'
                if n.startswith('K') and len(n) >= 3:
                    return n[:3]
                return n[0]
            df['team'] = df['AssignTo'].apply(get_team)
            df['team'] = df['team'].replace('', 'Unknown').fillna('Unknown')
        else:
            df['team'] = 'Unknown'
        return df

    def get_data(self):
        now_ist = datetime.now(IST_TZ)
        time_range = f"{now_ist.strftime('%Y-%m-%d 06:00:00')} - {now_ist.strftime('%Y-%m-%d 23:59:59')}"
        params = {
            "ctime_range": time_range,
            "key": self.conf['key'],
            "export_type": "excel",
            "p": "1",
            "limit": "10000"
        }
        print(f"📡 [DEBUG] 正在请求 API 数据: {time_range}", flush=True)
        try:
            resp = requests.get(self.conf['base_url'], params=params, headers=self.headers, timeout=60, verify=False)
            print(f"📡 [DEBUG] API 响应状态码: {resp.status_code}", flush=True)
            
            if 'html' in resp.headers.get('Content-Type', '').lower():
                print("❌ [DEBUG] API 返回了 HTML，可能是 Cookie 过期！", flush=True)
                return None
            
            try:
                df = pd.read_excel(BytesIO(resp.content))
            except:
                df = pd.read_csv(BytesIO(resp.content), sep=None, engine='python')
            
            if df is None or df.empty:
                print("⚠️ [DEBUG] API 返回的 DataFrame 为空", flush=True)
                return pd.DataFrame()

            print(f"✅ [DEBUG] API 数据获取成功，行数: {len(df)}", flush=True)
            
            # 逻辑同步：更健壮的列名处理（防止API变动）
            df.columns = [str(c).strip() for c in df.columns]
            if 'APP' in df.columns: df = df.rename(columns={'APP': 'App'})
            
            col_map = {
                'Date': 'TimePoint', 'Ticket Category': 'Stage', 'Is Reloan': 'LoanType',
                'Assign To': 'AssignTo', 'Total Left Unpaid Principal': 'TotalLeft',
                'Total Repay Amount': 'TotalRepayAmount',
                'Load Num': 'LoadNum'
            }
            
            # 兜底逻辑：如果找不到关键列，尝试按索引重命名
            if 'AssignTo' not in df.columns and 'Assign To' not in df.columns:
                 if df.shape[1] >= 20:
                    # 尝试保留前20列并重命名
                    df = df.iloc[:, :20]
                    expected = ['id','EmployeeID','TimePoint','Stage','LoanType','role','Ranking','AssignTo',
                                'TotalLeft','RepayPrincipal','RepayInterest','RepayServiceFee',
                                'TotalRepayAmount','RepayRate','TargetRepayRate','NewAssignNum',
                                'HandleNum','CompleteNum','LoadNum','App']
                    if len(df.columns) == len(expected):
                        df.columns = expected

            col_map['Complete Num'] = 'CompleteNum'
            df = df.rename(columns=col_map)
            
            # 逻辑同步：标准化 Stage 和 LoanType
            if 'Stage' in df.columns:
                df['Stage'] = df['Stage'].astype(str).str.strip().str.upper()
                df['Stage'] = df['Stage'].replace({'RM1': 'RM1', 'RM0': 'RM0', 'D0': 'D0', 'M11': 'M1-1'})
                
            if 'LoanType' in df.columns:
                df['LoanType'] = df['LoanType'].astype(str).str.strip()
                # 统一映射：New -> First loan, Old -> Reloan
                type_map = {
                    'New': 'First loan', '1': 'First loan', '0': 'Reloan',
                    'Old': 'Reloan', 'FIRST LOAN': 'First loan', 'RELOAN': 'Reloan'
                }
                df['LoanType'] = df['LoanType'].map(lambda x: type_map.get(x.upper(), x) if isinstance(x, str) else x)
                # 再次兜底映射以防万一
                df['LoanType'] = df['LoanType'].replace({'New': 'First loan', 'Old': 'Reloan'})

            df_final = self.process_team_assignment(df)
            return df_final
        except Exception as e:
            print(f"❌ [DEBUG] 数据获取异常: {e}", flush=True)
            return None

    def get_op_logs(self):
        # 计算 2 小时窗口 (IST)
        now_ist = datetime.now(IST_TZ)
        end_ist = now_ist.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        start_ist = end_ist - timedelta(hours=2)
        time_range_str = f"{start_ist.strftime('%Y-%m-%d %H:00')} - {end_ist.strftime('%Y-%m-%d %H:00')}"
        
        target_url = 'https://dc.tidbi-it.cc/admin/uri_access/list'
        params = {
            'uid': '',
            'nickname': '',
            'path': '',
            'time_range': time_range_str,
            'download': '1'
        }
        
        print(f"📡 [DEBUG] 正在获取操作记录: {time_range_str}", flush=True)
        try:
            # 尝试第一个数据源 (URI Access Logs)
            resp = requests.get(target_url, params=params, headers=self.headers, timeout=60, verify=False)
            
            # 如果第一个数据源没拿到数据，尝试用户提供的第二个数据源 (Ticket List)
            if resp.status_code != 200 or len(resp.content) < 500:
                print(f"⚠️ [DEBUG] uri_access/list 获取失败或数据过少，尝试 ticket/list...", flush=True)
                ticket_url = "https://dc.tidbi-it.cc/ticket/list"
                ticket_params = {
                    "ctime_range": time_range_str,
                    "export": "1",
                    "submit": "true"
                }
                resp = requests.get(ticket_url, params=ticket_params, headers=self.headers, timeout=60, verify=False)

            if resp.status_code != 200:
                print(f"⚠️ [DEBUG] 所有操作记录数据源均获取失败，状态码: {resp.status_code}", flush=True)
                return {}
            
            content_str = resp.text
            if content_str.strip().startswith('<') or '<html' in content_str.lower():
                from io import StringIO
                dfs = pd.read_html(StringIO(content_str))
                if not dfs: return {}
                df = dfs[0]
            else:
                try:
                    df = pd.read_excel(BytesIO(resp.content))
                except:
                    df = pd.read_csv(BytesIO(resp.content), sep=None, engine='python')
            
            if df is None or df.empty:
                print("⚠️ [DEBUG] 操作记录 DataFrame 为空", flush=True)
                return {}
            
            # 统一列名为字符串，移除 BOM 字节序标记，并去除首尾空格
            df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
            
            # 寻找 Nickname 和 Time 列 (不区分大小写，增加常用别名)
            nick_col = next((c for c in df.columns if c.lower() in ['nickname', 'assignto', 'assign to', 'worker', 'employee']), None)
            time_col = next((c for c in df.columns if c.lower() in ['time', 'createdtime', 'updatedtime', 'date', 'timepoint', 'created time']), None)
            
            if not nick_col or not time_col:
                print(f"⚠️ [DEBUG] 操作记录缺少必要列。现有列: {list(df.columns)}", flush=True)
                return {}
            
            # 获取小时信息 (格式化为两位数)
            df['Hour'] = pd.to_datetime(df[time_col], errors='coerce').dt.strftime('%H')
            
            # 统计每个人的每小时操作次数
            pt = pd.pivot_table(df, index=nick_col, columns='Hour', aggfunc='size', fill_value=0)
            pt['TotalOpTimes'] = pt.sum(axis=1)
            
            # 转换为字典
            op_data = pt.to_dict('index')
            
            # 获取小时标签
            hours = [str(h) for h in pt.columns if h != 'TotalOpTimes']
            hours.sort()
            
            print(f"✅ [DEBUG] 成功解析操作记录，覆盖 {len(op_data)} 人，小时标签: {hours}", flush=True)
            return {"data": op_data, "hours": hours}
        except Exception as e:
            print(f"❌ [DEBUG] 操作记录处理异常: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return {}

class DataAnalyzer:
    def __init__(self, history_filename):
        self.bucket_name = CONFIG['system'].get('history_bucket')
        self.blob_name = history_filename
        self.history_list = []
        self.current_snapshot = {}
        self.storage_client = None
        self.bucket = None
        
        print(f"📦 [DEBUG] 正在初始化 GCS，Bucket: {self.bucket_name}", flush=True)
        if self.bucket_name:
            try:
                self.storage_client = storage.Client()
                self.bucket = self.storage_client.bucket(self.bucket_name)
                self.history_list = self._load_history()
            except Exception as e:
                print(f"❌ [DEBUG] GCS 初始化失败: {e}", flush=True)
        else:
            print("⚠️ [DEBUG] 未配置 history_bucket", flush=True)

        self.lagging_threshold_ratio = CONFIG['system'].get('lagging_threshold', 0.9)
        self.stagnation_hours = CONFIG['system'].get('stagnation_hours', 2)

    def _load_history(self):
        if not self.bucket: return []
        try:
            blob = self.bucket.blob(self.blob_name)
            if not blob.exists():
                print(f"ℹ️ [DEBUG] GCS 文件 {self.blob_name} 不存在，将新建", flush=True)
                return []
            content = blob.download_as_text(encoding='utf-8')
            data = json.loads(content)
            
            # 逻辑同步：只保留当天的历史数据
            if isinstance(data, list):
                today_str = datetime.now(IST_TZ).strftime('%Y-%m-%d')
                filtered_data = [
                    h for h in data 
                    if datetime.fromtimestamp(h["timestamp"], IST_TZ).strftime('%Y-%m-%d') == today_str
                ]
                print(f"✅ [DEBUG] 成功加载今日历史记录，条数: {len(filtered_data)}", flush=True)
                return filtered_data
            return []
        except Exception as e: 
            print(f"❌ [DEBUG] 加载历史记录失败: {e}", flush=True)
            return []

    def save_history(self):
        if not self.current_snapshot: return
        now_ist = datetime.now(IST_TZ)
        current_ts = time.time()
        
        self.history_list.append({"timestamp": current_ts, "data": self.current_snapshot})
        
        # 只保留当天
        today_str = datetime.now(IST_TZ).strftime('%Y-%m-%d')
        self.history_list = [
            h for h in self.history_list 
            if datetime.fromtimestamp(h["timestamp"], IST_TZ).strftime('%Y-%m-%d') == today_str
        ]

        if not self.bucket: return
        try:
            blob = self.bucket.blob(self.blob_name)
            blob.upload_from_string(json.dumps(self.history_list, ensure_ascii=False, indent=2), content_type='application/json')
            print(f"✅ [DEBUG] 历史记录已保存到 GCS", flush=True)
        except Exception as e:
            print(f"❌ [DEBUG] 保存历史记录失败: {e}", flush=True)

    def get_last_run_data(self, key):
        if len(self.history_list) < 1: return None
        last_record = self.history_list[-1]
        return last_record["data"].get(key)

    def get_data_2h_ago(self, key):
        if not self.history_list: return None
        target_time = time.time() - (self.stagnation_hours * 3600)
        best_record = None
        min_diff = 3600 * 1.5
        for record in self.history_list:
            diff = abs(record["timestamp"] - target_time)
            if diff < min_diff:
                min_diff = diff
                best_record = record
        if best_record: return best_record["data"].get(key)
        return None

    def record_snapshot(self, key, rate, repay_amount):
        self.current_snapshot[key] = {'rate': float(rate), 'repay': float(repay_amount)}

    def calculate_global_averages(self, df):
        df_calc = df.copy()
        for c in ['TotalLeft', 'TotalRepayAmount', 'TimePoint']:
            if c in df_calc.columns: df_calc[c] = pd.to_numeric(df_calc[c], errors='coerce').fillna(0)
        if 'TimePoint' in df_calc.columns and not df_calc.empty:
            max_time = df_calc['TimePoint'].max()
            df_calc = df_calc[df_calc['TimePoint'] == max_time]
        
        # 确保 Stage 和 LoanType 已经是标准格式
        df_calc['Stage'] = df_calc['Stage'].astype(str).str.strip()
        df_calc['LoanType'] = df_calc['LoanType'].astype(str).str.strip()
        
        global_stats = df_calc.groupby(['Stage', 'LoanType'])[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
        global_avg_map = {}
        for _, row in global_stats.iterrows():
            rate = (row['TotalRepayAmount'] / row['TotalLeft'] * 100) if row['TotalLeft'] > 0 else 0
            global_avg_map[(row['Stage'], row['LoanType'])] = rate
        return global_avg_map
    
    def calculate_global_team_ranks(self, df):
        df_calc = df.copy()
        for c in ['TotalLeft', 'TotalRepayAmount', 'TimePoint']:
            if c in df_calc.columns: df_calc[c] = pd.to_numeric(df_calc[c], errors='coerce').fillna(0)
        if 'TimePoint' in df_calc.columns and not df_calc.empty:
            max_time = df_calc['TimePoint'].max()
            df_calc = df_calc[df_calc['TimePoint'] == max_time]
            
        df_calc['Stage'] = df_calc['Stage'].astype(str).str.strip()
        df_calc['LoanType'] = df_calc['LoanType'].astype(str).str.strip()
        
        team_stats = df_calc.groupby(['Stage', 'LoanType', 'team'])[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
        team_stats['Rate'] = (team_stats['TotalRepayAmount'] / team_stats['TotalLeft'] * 100).fillna(0)
        rank_lookup = {}
        for (stage, ltype), group in team_stats.groupby(['Stage', 'LoanType']):
            group = group.sort_values(by='Rate', ascending=False).reset_index(drop=True)
            total_teams = len(group)
            for rank, row in group.iterrows():
                rank_str = f"{rank + 1}/{total_teams}"
                rank_lookup[(stage, ltype, str(row['team']).strip())] = rank_str
        return rank_lookup
    
    def calculate_global_person_ranks(self, df):
        df_calc = df.copy()
        for c in ['TotalLeft', 'TotalRepayAmount', 'TimePoint']:
            if c in df_calc.columns: df_calc[c] = pd.to_numeric(df_calc[c], errors='coerce').fillna(0)
        if 'TimePoint' in df_calc.columns and not df_calc.empty:
            max_time = df_calc['TimePoint'].max()
            df_calc = df_calc[df_calc['TimePoint'] == max_time]
            
        df_calc['Stage'] = df_calc['Stage'].astype(str).str.strip()
        df_calc['LoanType'] = df_calc['LoanType'].astype(str).str.strip()
        
        # 1. 计算 RM1 的排名 (包含 App)
        df_rm1 = df_calc[df_calc['Stage'] == 'RM1'].copy()
        rank_lookup = {}
        if not df_rm1.empty:
            person_stats_rm1 = df_rm1.groupby(['Stage', 'LoanType', 'App', 'AssignTo'])[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
            person_stats_rm1['Rate'] = (person_stats_rm1['TotalRepayAmount'] / person_stats_rm1['TotalLeft'] * 100).fillna(0)
            
            for (stage, ltype, app), group in person_stats_rm1.groupby(['Stage', 'LoanType', 'App']):
                group = group.sort_values(by='Rate', ascending=False).reset_index(drop=True)
                total_persons = len(group)
                for rank, row in group.iterrows():
                    rank_lookup[(stage, ltype, str(app).strip(), str(row['AssignTo']).strip())] = f"{rank + 1}/{total_persons}"

        # 2. 计算其他阶段的排名 (不包含 App)
        df_others = df_calc[df_calc['Stage'] != 'RM1'].copy()
        if not df_others.empty:
            person_stats_others = df_others.groupby(['Stage', 'LoanType', 'AssignTo'])[['TotalLeft', 'TotalRepayAmount']].sum().reset_index()
            person_stats_others['Rate'] = (person_stats_others['TotalRepayAmount'] / person_stats_others['TotalLeft'] * 100).fillna(0)
            
            for (stage, ltype), group in person_stats_others.groupby(['Stage', 'LoanType']):
                group = group.sort_values(by='Rate', ascending=False).reset_index(drop=True)
                total_persons = len(group)
                for rank, row in group.iterrows():
                    rank_lookup[(stage, ltype, str(row['AssignTo']).strip())] = f"{rank + 1}/{total_persons}"
                    
        return rank_lookup

    # 逻辑同步：引入 _get_stats_block 辅助函数
    def _get_stats_block(self, df_sub, row_name, stage, type_, snapshot_key, current_hour, global_ranks_dict=None, team_rank_id=None):
        t_left = df_sub['TotalLeft'].sum()
        t_repay = df_sub['TotalRepayAmount'].sum()
        rate = (t_repay / t_left * 100) if t_left > 0 else 0.0
        
        self.record_snapshot(snapshot_key, rate, t_repay)
        last_d = self.get_last_run_data(snapshot_key)
        last_r = last_d.get('rate', 0.0) if last_d else 0.0
        
        diff = rate - last_r
        
        t_load = df_sub['LoadNum'].sum() if 'LoadNum' in df_sub.columns else 0
        t_complete = df_sub['CompleteNum'].sum() if 'CompleteNum' in df_sub.columns else 0
        t_re_pct = (t_complete / t_load * 100) if t_load > 0 else 0.0
        t_re_pct_str = f"{t_re_pct:.1f}%"
        
        target_str = "-"
        achv_str = "-"
        if current_hour is not None:
            tr_raw = TARGET_RATES.get((stage, type_), {}).get(current_hour)
            if tr_raw is not None:
                tr_pct = tr_raw * 100
                target_str = f"{tr_pct:.1f}%"
                if tr_pct > 0:
                    achv_str = f"{(rate / tr_pct * 100):.0f}%"
        
        rank_str = "-"
        if global_ranks_dict and team_rank_id:
            rank_key = (stage, type_, str(team_rank_id))
            rank_str = global_ranks_dict.get(rank_key, "-")
        
        app_str = ""
        if 'App' in df_sub.columns:
            app_str = ",".join([str(a) for a in df_sub['App'].dropna().unique()])
            
        sub_name = "-"
        if str(row_name).startswith('App: '):
            sub_name = "Team"
        elif 'team' in df_sub.columns:
            teams = df_sub['team'].dropna().unique()
            if len(teams) == 1:
                sub_name = str(teams[0])
            
        return {
            "Stage": stage, "Type": type_, "Name": row_name, "SubName": sub_name, "App": app_str,
            "Re%": f"{rate:.1f}%", "Target": target_str, "Achv%": achv_str,
            "Diff.Avg.Re%": "-", 
            "GlobalRank": rank_str, 
            "T.Re%": t_re_pct_str,
            "Tickets": int(df_sub['LoadNum'].sum()), 
            "Left": int(t_left), "RateNum": rate
        }

    # 逻辑同步：重写 process_team_data 以匹配 auto_report.py
    def process_team_data(self, df, team_id, team_name, global_ranks, global_averages, global_person_ranks, op_logs=None):
        # 兼容性修复：如果配置的ID是None，视为ALL
        if team_id is None or str(team_id).lower() == 'none':
            target_id = 'ALL'
        else:
            target_id = str(team_id).strip()
            
        is_all_report = (target_id.upper() == 'ALL')
        if is_all_report:
            df_team = df.copy()
        elif target_id.upper() == 'K':
            df_team = df[df['team'].astype(str).str.strip().str.startswith('K')].copy()
        else:
            df_team = df[df['team'].astype(str).str.strip() == target_id].copy()
            
        if df_team.empty: return None, None

        current_hour = None
        if 'TimePoint' in df_team.columns:
            df_team['TimePoint'] = pd.to_numeric(df_team['TimePoint'], errors='coerce')
            max_time = df_team['TimePoint'].max()
            df_latest = df_team[df_team['TimePoint'] == max_time].copy()
            if pd.notna(max_time):
                try:
                    current_hour = int(str(int(max_time))[-2:])
                except: pass
        else:
            df_latest = df_team.copy()
            
        for c in ['TotalLeft', 'TotalRepayAmount', 'LoadNum', 'CompleteNum']:
            if c in df_latest.columns: df_latest[c] = pd.to_numeric(df_latest[c], errors='coerce').fillna(0)
        df_latest['Stage'] = df_latest['Stage'].astype(str).str.strip()
        df_latest['LoanType'] = df_latest['LoanType'].astype(str).str.strip()
        df_latest = df_latest[~df_latest['Stage'].str.upper().str.startswith('M1')]

        plot_data_rm1 = []
        plot_data_rm0 = []
        grouped = df_latest.groupby(['Stage', 'LoanType'])
        groups_list = list(grouped)
        
        def custom_sort_key(item):
            stage, ltype = item[0]
            s_score = 0 if stage.upper() == 'RM1' else (1 if stage.upper() == 'RM0' else 2)
            t_score = 0 if 'FIRST' in ltype.upper() else (1 if 'RELOAN' in ltype.upper() else 2)
            return (s_score, t_score)
        groups_list.sort(key=custom_sort_key)
        
        for idx, ((stage, type_), group) in enumerate(groups_list):
            target_list = plot_data_rm1 if 'RM1' in stage.upper() else plot_data_rm0
            
            def get_grouped_blocks(df_target, prefix):
                blocks = []
                unique_teams = df_target['team'].dropna().unique()
                k_teams = [t for t in unique_teams if str(t).startswith('K-')]
                other_teams = [t for t in unique_teams if not str(t).startswith('K-') and str(t) != 'Unknown']
                
                name_k = f"Team K" if prefix == "Team" else f"{prefix} - K"
                
                for t in other_teams:
                    sub_df = df_target[df_target['team'] == t]
                    name = f"Team {t}" if prefix == "Team" else f"{prefix} - {t}"
                    key = f"ALL_{prefix}_{stage}_{type_}_{t}"
                    blocks.append(self._get_stats_block(sub_df, name, stage, type_, key, current_hour, global_ranks, t))
                
                if k_teams:
                    df_k = df_target[df_target['team'].isin(k_teams)]
                    key_k = f"ALL_{prefix}_{stage}_{type_}_K_TOTAL"
                    block_k = self._get_stats_block(df_k, name_k, stage, type_, key_k, current_hour, global_ranks, "K")
                    block_k['SubName'] = "K"
                    blocks.append(block_k)
                    
                blocks.sort(key=lambda x: x['RateNum'], reverse=True)
                
                total_major = len(blocks)
                for i, b in enumerate(blocks):
                    b['GlobalRank'] = f"{i+1}/{total_major}"
                
                if k_teams:
                    k_sub_blocks = []
                    for t in k_teams:
                        sub_df = df_target[df_target['team'] == t]
                        name = f"Team {t}" if prefix == "Team" else f"{prefix} - {t}"
                        key = f"ALL_{prefix}_{stage}_{type_}_{t}"
                        k_sub_blocks.append(self._get_stats_block(sub_df, name, stage, type_, key, current_hour, global_ranks, t))
                    
                    k_sub_blocks.sort(key=lambda x: x['RateNum'], reverse=True)
                    
                    total_sub = len(k_sub_blocks)
                    for i, b in enumerate(k_sub_blocks):
                        b['GlobalRank'] = f"{i+1}/{total_sub}"
                        
                    k_idx = next((i for i, b in enumerate(blocks) if b['Name'] == name_k), -1)
                    if k_idx != -1:
                        blocks = blocks[:k_idx+1] + k_sub_blocks + blocks[k_idx+1:]
                
                return blocks

            if is_all_report:
                if stage.upper() != 'RM1':
                    # 1. Overall Total
                    target_list.append(self._get_stats_block(group, "TOTAL", stage, type_, f"ALL_{stage}_{type_}_TOTAL", current_hour))
                    
                    # 2. Per Team Breakdown
                    team_blocks = get_grouped_blocks(group, "Team")
                    target_list.extend(team_blocks)
                
                # 3. Per App Breakdown
                unique_apps = sorted(group['App'].dropna().unique())
                for app_name in unique_apps:
                    app_str = str(app_name).strip()
                    if not app_str: continue
                    app_df = group[group['App'] == app_name]
                    if app_df.empty: continue
                    
                    target_list.append(self._get_stats_block(app_df, f"App: {app_str}", stage, type_, f"ALL_{app_str}_{stage}_{type_}_TOTAL", current_hour))
                    
                    app_team_blocks = get_grouped_blocks(app_df, app_str)
                    target_list.extend(app_team_blocks)

            else:
                # Team 汇总
                target_list.append(self._get_stats_block(group, f"Team {team_id}", stage, type_, f"{team_id}_{stage}_{type_}_GROUP", current_hour, global_ranks, team_id))
                
                # 员工详情 (逻辑同步)
                global_avg = global_averages.get((stage, type_), 0.0)
                target_pct = 0.0
                target_str = "-"
                if current_hour is not None:
                    tr = TARGET_RATES.get((stage, type_), {}).get(current_hour)
                    if tr: 
                        target_pct = tr * 100
                        target_str = f"{target_pct:.1f}%"

                if 'App' in group.columns:
                    person_stats = group.groupby('AssignTo').agg({
                        'TotalLeft': 'sum',
                        'TotalRepayAmount': 'sum',
                        'LoadNum': 'sum',
                        'CompleteNum': 'sum',
                        'App': lambda x: ','.join(x.dropna().astype(str).unique()),
                        'team': 'first'
                    }).reset_index()
                else:
                    person_stats = group.groupby('AssignTo').agg({
                        'TotalLeft': 'sum',
                        'TotalRepayAmount': 'sum',
                        'LoadNum': 'sum',
                        'CompleteNum': 'sum',
                        'team': 'first'
                    }).reset_index()
                    person_stats['App'] = ""
                    
                person_stats['Rate'] = (person_stats['TotalRepayAmount'] / person_stats['TotalLeft'] * 100).fillna(0)
                
                person_blocks = []
                for _, row in person_stats.iterrows():
                    name = str(row['AssignTo']).strip()
                    p_rate = row['Rate']
                    
                    p_key = f"{team_id}_{stage}_{type_}_{name}"
                    self.record_snapshot(p_key, p_rate, row['TotalRepayAmount'])
                    last_pd = self.get_last_run_data(p_key)
                    last_pr = last_pd.get('rate', 0.0) if last_pd else 0.0
                    
                    diff = p_rate - last_pr
                    if abs(diff) >= 0.01:
                        sym = "⬆️" if diff > 0 else "⬇️"
                        p_trend = f"{sym} {abs(diff):.1f}%"
                    else: p_trend = "-"
                    
                    # Lagging / Stagnant 判定逻辑
                    is_lagging = False
                    if global_avg > 0 and p_rate < global_avg * self.lagging_threshold_ratio: is_lagging = True
                    elif p_rate == 0 and row['TotalLeft'] > 0: is_lagging = True
                    
                    is_stagnant = False
                    if is_lagging:
                        data_2h = self.get_data_2h_ago(p_key)
                        if data_2h and abs(row['TotalRepayAmount'] - data_2h.get('repay', 0.0)) < 1.0:
                            is_stagnant = True
                    
                    # 根据阶段决定排名匹配逻辑
                    if stage == 'RM1':
                        # RM1 阶段根据 App 细分排名
                        primary_app = str(row.get('App', '')).split(',')[0].strip()
                        p_rank_key = (stage, type_, primary_app, name)
                    else:
                        # 其他阶段 (如 RM0) 只根据 Stage 和 Type 排名，不区分 App
                        p_rank_key = (stage, type_, name)
                        
                    p_rank = global_person_ranks.get(p_rank_key, "N/A")
                    
                    p_t_load = row.get('LoadNum', 0)
                    p_t_complete = row.get('CompleteNum', 0)
                    p_t_re_pct = (p_t_complete / p_t_load * 100) if p_t_load > 0 else 0.0
                    p_t_re_pct_str = f"{p_t_re_pct:.1f}%"
                    
                    p_stats_row = {
                        "Stage": stage, "Type": type_, "Name": name, "SubName": str(row.get('team', '-')), "App": str(row.get('App', '')),
                        "Re%": f"{p_rate:.1f}%", "Target": target_str,
                        "GlobalRank": p_rank,
                        "T.Re%": p_t_re_pct_str,
                        "Tickets": int(row['LoadNum']), "Left": int(row['TotalLeft']), "RateNum": p_rate
                    }

                    # 处理操作次数详情
                    if op_logs and 'data' in op_logs:
                        person_op = op_logs['data'].get(name, {})
                        p_stats_row["OpTimes"] = person_op.get('TotalOpTimes', 0)
                        for h_label in op_logs.get('hours', []):
                            p_stats_row[f"H_{h_label}"] = person_op.get(h_label, 0)
                    else:
                        p_stats_row["OpTimes"] = "-"

                    person_blocks.append(p_stats_row)
                    
                person_blocks.sort(key=lambda x: x['RateNum'], reverse=True)
                target_list.extend(person_blocks)

        return {
            "RM1": pd.DataFrame(plot_data_rm1),
            "RM0": pd.DataFrame(plot_data_rm0)
        }, current_hour

async def generate_image(df_plot, team_id, suffix="", title=""):
    if df_plot is None or df_plot.empty: return None
    filename = f"report_{team_id}_{suffix}.png"
    
    if not df_plot.empty:
        if 'GlobalRank' in df_plot.columns: df_plot = df_plot.rename(columns={'GlobalRank': 'Rank'})
        display_cols = ['Stage', 'Type', 'Name', 'Re%', 'Target', 'Achv%', 'Diff.Avg.Re%', 'T.Re%', 'Rank', 'Tickets', 'RateNum', 'Left']
        df_plot = df_plot[[c for c in display_cols if c in df_plot.columns]]
        
    def highlight_stages(row):
        stage = str(row['Stage']).strip()
        type_ = str(row['Type']).strip()
        name = str(row['Name']).strip()
        color = '#ffffff'
        
        # 逻辑同步：精确匹配 auto_report.py 的颜色逻辑
        if stage == 'RM0' and type_ == 'First loan': color = '#e6f7ff'
        elif stage == 'RM0' and type_ == 'Reloan': color = '#f9f0ff'
        elif stage == 'RM1' and type_ == 'First loan': color = '#fffbe6'
        elif stage == 'RM1' and type_ == 'Reloan': color = '#f6ffed'
        
        if 'CashDaily' in name: color = '#ffe6e6'
        
        font_weight = 'normal'
        if name == 'TOTAL' or name.startswith('App:'):
            color = '#dcdcdc'
            font_weight = 'bold'
        elif name.startswith('Team '):
            font_weight = 'bold'
            
        return [f'background-color: {color}; font-weight: {font_weight}' for _ in row]

    def _sync_export():
        try:
            styler = df_plot.style\
                .apply(highlight_stages, axis=1)\
                .background_gradient(subset=['RateNum'], cmap='RdYlGn', vmin=0, vmax=60)\
                .hide(axis='index')\
                .hide(subset=['RateNum', 'Left'], axis='columns') \
                .set_properties(**{'border': '1px solid gray', 'text-align': 'center', 'font-size': '12pt'})\
                .set_caption(title)\
                .set_table_styles([
                    {'selector': 'th', 'props': [('background-color', '#2c3e50'), ('color', 'white'), ('font-weight', 'bold')]},
                    {'selector': 'caption', 'props': [('caption-side', 'top'), ('font-size', '16px'), ('font-weight', 'bold'), ('color', 'black'), ('text-align', 'left'), ('margin-bottom', '10px')]}
                ])
            dfi.export(styler, filename, max_rows=1000)
            return filename
        except Exception as e:
            print(f"❌ [DEBUG] 图片生成失败: {e}", flush=True)
            return None

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _sync_export)
        return result
    except Exception as e:
        print(f"❌ [DEBUG] 线程池调度失败: {e}", flush=True)
        return None

async def send_to_telegram(bot_token, chat_id, text, img_path):
    bot = Bot(token=bot_token, request=HTTPXRequest())
    try:
        if img_path and os.path.exists(img_path):
            with open(img_path, 'rb') as f:
                if text: await bot.send_photo(chat_id=chat_id, photo=f, caption=text, parse_mode='HTML')
                else: await bot.send_photo(chat_id=chat_id, photo=f)
            os.remove(img_path)
        print(f"✅ [DEBUG] 消息已推送至 {chat_id}", flush=True)
    except Exception as e:
        print(f"❌ [DEBUG] 推送失败: {e}", flush=True)

async def run_cycle():
    print("🔥 [DEBUG] 开始执行报表任务...", flush=True)

    now_ist = datetime.now(IST_TZ)
    termination_hour = CONFIG['system'].get('termination_hour', 22)
    
    print(f"⏰ [DEBUG] 当前时间(IST): {now_ist.strftime('%H:%M')}, 终止时间: {termination_hour}:00", flush=True)
    
    if now_ist.hour >= termination_hour:
        print(f"🛑 [DEBUG] 已过终止时间，停止执行。", flush=True)
        return
    
    if 'api' not in CONFIG:
        print("❌ [DEBUG] 配置文件缺少 'api' 字段", flush=True)
        return

    api = APIClient(CONFIG['api'])
    history_file = CONFIG['system'].get('history_file', 'history.json')
    analyzer = DataAnalyzer(history_file)
    
    df = api.get_data()
    if df is None or df.empty:
        print("⚠️ [DEBUG] API 未返回数据，停止本次任务。", flush=True)
        return

    global_avgs = analyzer.calculate_global_averages(df)
    global_team_ranks = analyzer.calculate_global_team_ranks(df)
    global_person_ranks = analyzer.calculate_global_person_ranks(df)

    # 兼容性处理：无论 config 用的是 teams 还是 tasks
    teams = CONFIG.get('teams', CONFIG.get('tasks', []))
    if not teams:
        print("⚠️ [DEBUG] 未定义 teams/tasks 列表", flush=True)

    for team_conf in teams:
        # 兼容 key 名
        team_id = team_conf.get('id', team_conf.get('team_id'))
        name = team_conf.get('name')
        bot_token = team_conf.get('bot_token')
        chat_id = team_conf.get('chat_id')

        print(f"🔍 [DEBUG] 正在处理团队: {name} (ID: {team_id})", flush=True)

        dfs, current_hour = analyzer.process_team_data(
            df, team_id, name, global_team_ranks, global_avgs, global_person_ranks
        )

        if not dfs:
            print(f"⚠️ [DEBUG] 团队 {name} 无有效数据", flush=True)
            continue

        img_paths = []
        for stage_key in ['RM1', 'RM0']:
            sub_df = dfs.get(stage_key)
            if sub_df is not None and not sub_df.empty:
                ist_time_str = datetime.now(IST_TZ).strftime("%Y%m%d %H:%M")
                target_time_str = f"{current_hour}:00" if current_hour is not None else "N/A"
                
                # 兼容显示：如果是ALL模式，显示"总绩效"
                is_all = (str(team_id).upper() == 'ALL') or (team_id is None) or (str(team_id).lower() == 'none')
                display_name = "总绩效" if is_all else f"Team {name}"
                
                title_text = f"{display_name} Hourly Collection Ranking - {ist_time_str} (IST)\nTime: {target_time_str} Target Comparison"

                path = await generate_image(
                    sub_df, 
                    str(team_id), # 确保是字符串
                    suffix=stage_key, 
                    title=title_text
                )
                if path:
                    img_paths.append(path)

        try:
            if not img_paths:
                print(f"⚠️ [DEBUG] 团队 {name} 没有生成任何图片", flush=True)
            for img in img_paths:
                # auto_report 逻辑：不发送文字 caption，只发图片
                if bot_token and chat_id:
                    await send_to_telegram(bot_token, chat_id, "", img)
                else:
                    print(f"ℹ️ [DEBUG] 团队 {name} 未配置 bot_token/chat_id，跳过推送到 Telegram", flush=True)
        except Exception as e:
            print(f"❌ [DEBUG] 推送异常: {e}", flush=True)

    analyzer.save_history()
    print("✅ [DEBUG] 所有团队报表任务执行完毕", flush=True)

async def get_dashboard_data(team_id="ALL"):
    print(f"🔥 [DEBUG] 开始执行实时数据抓取(Dashboard), Team: {team_id}...", flush=True)
    if 'api' not in CONFIG: return {"status": "error", "message": "Missing API config"}

    api = APIClient(CONFIG['api'])
    history_file = CONFIG['system'].get('history_file', 'history.json')
    analyzer = DataAnalyzer(history_file)
    
    df = api.get_data()
    if df is None or df.empty:
        return {"status": "error", "message": "API 返回数据为空，可能是 Cookie 过期"}

    global_avgs = analyzer.calculate_global_averages(df)
    global_team_ranks = analyzer.calculate_global_team_ranks(df)
    global_person_ranks = analyzer.calculate_global_person_ranks(df)

    # 实时抓取操作记录
    op_logs = api.get_op_logs()

    dfs, current_hour = analyzer.process_team_data(
        df, team_id, f"Dashboard", global_team_ranks, global_avgs, global_person_ranks, op_logs=op_logs
    )

    if not dfs:
        return {"status": "error", "message": "无有效数据处理结果"}

    rm1_df = dfs.get("RM1")
    rm0_df = dfs.get("RM0")
    
    if rm1_df is not None: rm1_df = rm1_df.fillna("-")
    if rm0_df is not None: rm0_df = rm0_df.fillna("-")
    
    rm1_data = rm1_df.to_dict('records') if rm1_df is not None and not rm1_df.empty else []
    rm0_data = rm0_df.to_dict('records') if rm0_df is not None and not rm0_df.empty else []

    analyzer.save_history()

    now_ist = datetime.now(IST_TZ)
    return {
        "status": "success",
        "update_time": now_ist.strftime("%Y-%m-%d %H:%M:%S (IST)"),
        "current_hour_target": f"{current_hour}:00" if current_hour is not None else "-",
        "op_hours": op_logs.get('hours', []) if op_logs else [],
        "rm1": rm1_data,
        "rm0": rm0_data
    }


def main():
    print(">>> [DEBUG] 系统启动 (Single Run Mode)", flush=True)
    try:
        asyncio.run(run_cycle())
    except Exception as e:
        print(f"❌ [DEBUG] 全局异常: {e}", flush=True)
        raise e

if __name__ == "__main__":
    main()
