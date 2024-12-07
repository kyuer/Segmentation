# 建表结构
mysql -u root -o < user_ativity.sql
# 创建虚拟环境
python3 -m venv venv
# 切换虚拟环境
source venv/bin/activate 
# 安装运行所需环境
pip install -r requirements.txt
# 验证环境安装成功
python -c "import pyspark; import delta; print('Dependencies installed successfully!')"

