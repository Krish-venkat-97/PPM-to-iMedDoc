import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from libs import ConfigParser

config = ConfigParser() 
config.read('config.ini')
