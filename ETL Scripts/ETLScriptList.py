import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *

etl_scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ETL Scripts')
scripts = sorted([f for f in os.listdir(etl_scripts_dir) if f.endswith('.py') and not f.startswith('__')])

print("ETL scripts found:", scripts)