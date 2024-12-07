python3 -m venv venv
source venv/bin/activate 
pip install -r requirements.txt
python -c "import pyspark; import delta; print('Dependencies installed successfully!')"