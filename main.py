# main.py
import os
import asyncio
from flask import Flask, jsonify, request, render_template
from report_logic import run_cycle, logger

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    """渲染控制台显示页面"""
    return render_template('index.html')

@app.route('/api/status', methods=['GET', 'POST'])
def health_check():
    """健康检查接口"""
    return jsonify({"status": "OK", "service": "AutoReport Bot"}), 200

@app.route('/trigger', methods=['POST', 'GET'])
def trigger_report():
    """
    Cloud Scheduler 触发接口
    收到 POST 请求后，执行一次完整的报表生成与发送流程
    """
    mode = request.args.get('mode', 'regular')
    logger.info(f"收到触发请求，模式: {mode}，开始执行任务...")
    
    try:
        # 执行核心逻辑 (run_cycle 来自 report_logic.py)
        # 注意：run_cycle 是 async 函数，需要用 asyncio.run 运行
        result_msg = asyncio.run(run_cycle(run_mode=mode))
        
        status_code = 200
        if result_msg and "Error" in result_msg:
             status_code = 500
             
        return jsonify({"status": "Success", "message": result_msg or "Report cycle completed", "mode": mode}), status_code
    except Exception as e:
        logger.error(f"任务执行异常: {e}", exc_info=True)
        return jsonify({"status": "Error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    # 注意：Cloud Run 部署时通常由 Gunicorn 启动，这行代码主要用于本地调试
    app.run(debug=False, host='0.0.0.0', port=port)
