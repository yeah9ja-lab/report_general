# 1. 使用基于 Debian 12 (Bookworm) 的稳定版镜像，避免依赖包随时变动
FROM python:3.10-slim-bookworm

# 设置工作目录
WORKDIR /app

# 2. 安装基础工具 (只安装 wget 和 gnupg，浏览器依赖交给 playwright 处理)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# 3. 复制依赖文件并安装 Python 库
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 安装 Playwright 浏览器和它所需的系统依赖
# --with-deps 参数会自动安装所有缺少的 Linux 系统库 (libnss3, libgbm 等)
RUN playwright install chromium --with-deps

# 5. 复制其余代码
COPY . .

# 6. 设置启动命令
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
