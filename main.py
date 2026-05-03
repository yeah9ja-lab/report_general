import os
import sys
import asyncio
import traceback
import time  # <--- 必须加上这一行！放在最上面
from flask import Flask, jsonify, render_template, request

# 强制打印，确保日志能出来
print("🚀 [INIT] main.py 正在启动...", flush=True)

try:
    from report_logic import run_cycle, get_dashboard_data
    print("✅ [INIT] 成功导入 report_logic", flush=True)
except Exception as e:
    print(f"❌ [INIT] 导入 report_logic 失败: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1) # 导入失败直接退出容器

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/health', methods=['GET', 'POST'])
def health_check():
    return jsonify({"status": "OK", "service": "AutoReport Bot"}), 200

@app.route('/api/data', methods=['GET'])
def api_data():
    team_id = request.args.get('team', 'ALL')
    try:
        data = asyncio.run(get_dashboard_data(team_id))
        return jsonify(data), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/trigger', methods=['POST'])
def trigger_report():
    print("🔔 [TRIGGER] 收到请求，准备执行任务...", flush=True)
    try:
        # 记录开始时间
        start_time = time.time()
        
        # 执行核心逻辑
        asyncio.run(run_cycle())
        
        duration = time.time() - start_time
        print(f"✅ [TRIGGER] 任务执行完毕，耗时: {duration:.2f}秒", flush=True)
        return jsonify({"status": "Success", "message": "Report cycle completed"}), 200
    except Exception as e:
        error_msg = f"任务执行崩溃: {str(e)}"
        print(f"❌ [ERROR] {error_msg}", flush=True)
        # 打印完整的错误堆栈，这才是解决问题的关键
        traceback.print_exc()
        return jsonify({"status": "Error", "message": error_msg}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 [INIT] Flask 服务启动在端口 {port}", flush=True)
    app.run(debug=False, host='0.0.0.0', port=port)
