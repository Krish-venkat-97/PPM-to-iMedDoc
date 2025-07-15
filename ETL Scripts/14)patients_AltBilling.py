import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoices = 'SELECT * FROM InvoiceHeadSummary WHERE InvoiceTo = 2'

try:
    src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb_connection())
except:
    src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb2_connection())

src_alt_billing_df = src_invoices_df[['PatientCode', 'AccountName', 'AccountAddress1', 'AccountAddress2', 'AccountAddress3', 'AccountAddress4', 'AccountAddress5', 'AccountPostCode', 'AccountContactTelNo']]
src_alt_billing_df = src_alt_billing_df.drop_duplicates(subset=['PatientCode']).reset_index(drop=True)


#-------------------------inserting into target database---------------------
bar = tqdm(total=len(src_alt_billing_df), desc='Inserting Alternative Billing')

for index, row in src_alt_billing_df.iterrows():
    bar.update(1)
    update_alt_billing = f"""
    UPDATE patients p
    SET p.alt_billing_display_name = {safe_value(row['AccountName'])},
    p.alt_billing_address1 = {safe_value(row['AccountAddress1'])},
    p.alt_billing_address2 = {safe_value(row['AccountAddress2'])},
    p.alt_billing_address3 = {safe_value(row['AccountAddress3'])},
    p.alt_billing_address4 = {safe_value(row['AccountAddress4'])},
    p.alt_billing_county = {safe_value(row['AccountAddress5'])},
    p.alt_billing_postcode = {safe_value(row['AccountPostCode'])},
    p.alt_billing_phone = {safe_value(row['AccountContactTelNo'])}
    WHERE p.PPM_Patient_Id = {safe_value(row['PatientCode'])}
    """
    target_cursor.execute(update_alt_billing)

myconnection.commit()
myconnection.close()
bar.close()

