# 1. 使用官方 Python 3.10 轻量版作为基础镜像
FROM python:3.10-slim

# 2. 设置环境变量
# PYTHONUNBUFFERED 保证日志实时输出
# PORT 是 Cloud Run 默认使用的环境变量
ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    PIP_NO_CACHE_DIR=1

# 3. 设置工作目录
WORKDIR /app

# 4. 安装基础系统依赖和中文字体
# fonts-noto-cjk 解决中文乱码，fonts-noto-color-emoji 解决表情符号乱码
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-noto-cjk \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

# 5. 复制依赖描述文件
COPY requirements.txt .

# 6. 安装 Python 依赖
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 7. 关键步骤：安装 Playwright 浏览器及其所需的全部系统级依赖库
# --with-deps 会自动识别并安装运行 chromium 所需的所有 Linux 库
RUN playwright install --with-deps chromium

# 8. 复制当前目录下的所有程序代码到容器中
COPY . .

# 9. 暴露端口 (仅作声明)
EXPOSE 8080

# 10. 启动程序
# 使用 gunicorn 启动 Flask 应用
# --bind :8080 绑定端口
# --workers 1 限制进程数（Cloud Run 建议单进程多线程）
# --threads 8 提高并发处理能力
# --timeout 0 禁用超时限制，防止长耗时的报表生成任务被强杀
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
