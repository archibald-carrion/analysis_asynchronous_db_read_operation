Every python scripts used to create the drawing where made using DeepSeek R1 giving as context the conresponding documentation or scripts depending on the drawing.

python3 -m venv venv
source venv/bin/activate
cd drawings/
pip install -r requirements.txt
./drawing_3_methods.py
./drawing_db_setup.py
deactivate