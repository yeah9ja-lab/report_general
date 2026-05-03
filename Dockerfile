# 1. 使用基于 Debian 12 (Bookworm) 的稳定版镜像
FROM python:3.10-slim-bookworm

# 设置工作目录
WORKDIR /app

# 2. 安装基础工具和构建依赖 (lxml 等包可能需要 libxml2-dev)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    libxml2-dev \
    libxslt-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 3. 复制依赖文件并安装 Python 库
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 安装 Playwright 浏览器和它所需的系统依赖
# 注意：这里需要再次 update 以便 playwright install --with-deps 能安装系统库
RUN apt-get update && \
    python -m playwright install chromium --with-deps && \
    rm -rf /var/lib/apt/lists/*

# 5. 复制其余代码
COPY . .

# 6. 设置启动命令
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
