import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_insurance_companies = 'SELECT * FROM CodeInsuranceCompanys'

try:
    src_insurance_companies_df = pd.read_sql(src_insurance_companies, get_src_accessdb_connection())
except:
    src_insurance_companies_df = pd.read_sql(src_insurance_companies, get_src_accessdb2_connection())



print('debug')