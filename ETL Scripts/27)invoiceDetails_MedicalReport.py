import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

tgt_invoice_df = pd.read_sql('SELECT id as tgt_invoice_id,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL', myconnection)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)

src_medical_report = 'SELECT * FROM MedicalReportFile'
try:
    src_medical_report_df = pd.read_sql(src_medical_report, get_src_accessdb_connection())
except:
    src_medical_report_df = pd.read_sql(src_medical_report, get_src_accessdb2_connection())
src_medical_report_df = src_medical_report_df[src_medical_report_df['InvoiceNumber'] != 0]
src_medical_report_df['InvoiceNumber'] = src_medical_report_df['InvoiceNumber'].astype(int)

landing_medical_report_df = pd.merge(src_medical_report_df, tgt_invoice_df, left_on='InvoiceNumber', right_on='PPM_Invoice_Id', how='inner')
landing_medical_report_df = landing_medical_report_df.rename(columns={'tgt_invoice_id': 'invoice_id'})
landing_medical_report_df = landing_medical_report_df.drop(columns=['PPM_Invoice_Id', 'InvoiceNumber'])

landing_medical_report_df1 = landing_medical_report_df[['invoice_id','MedID','InvoiceDate','ReportCode','ReportCharge','VATAmount']]

#----------------------invoice date-----------------
def invoiceDate(row):
    if pd.isna(row['InvoiceDate']) or row['InvoiceDate'] == '':
        return None
    else:
        return row['InvoiceDate'].strftime('%Y-%m-%d') 
    
landing_medical_report_df1['InvoiceDate'] = landing_medical_report_df1.apply(invoiceDate, axis=1)

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoice_details'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
landing_medical_report_df1.insert(0, 'invoice_details_id', range(max_id, max_id + len(landing_medical_report_df1)))

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoice_details ADD COLUMN IF NOT EXISTS PPM_Invoice_MedRep_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#------------------------------Inserting data into invoice_details-----------------------------
invoice_medRep_df = landing_medical_report_df1

#-----------------------------amount-------------------------------
invoice_medRep_df['amount'] = invoice_medRep_df['ReportCharge'] - invoice_medRep_df['VATAmount']

#------------------------filtering out rows already present in target database------------------------
tgt_invoice_details_df = pd.read_sql('SELECT DISTINCT PPM_Invoice_MedRep_Id FROM invoice_details WHERE PPM_Invoice_MedRep_Id IS NOT NULL', myconnection)
invoice_medRep_df['MedID'] = invoice_medRep_df['MedID'].astype(str)
tgt_invoice_details_df['PPM_Invoice_MedRep_Id'] = tgt_invoice_details_df['PPM_Invoice_MedRep_Id'].astype(str)
invoice_medRep_df = invoice_medRep_df[~invoice_medRep_df['MedID'].isin(tgt_invoice_details_df['PPM_Invoice_MedRep_Id'])]

bar = tqdm(total=len(invoice_medRep_df), desc='Inserting Invoice Details from Medical Reports')

for index, row in invoice_medRep_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoice_details` (id, `invoice_id`, `procedure_date`, `procedure_id`, `procedure_code`, `procedure_name`, `service_location_id`, `qty`, `amount`, `total`, `inv_fee_split_percentage`, `inv_fee_split_amount`, `created_at`, `updated_at`, `deleted_at`, PPM_Invoice_MedRep_Id) 
        VALUES ({safe_value(row['invoice_details_id'])}, {safe_value(row['invoice_id'])}, {safe_value(row['InvoiceDate'])}, NULL, NULL, {safe_value(row['ReportCode'])}, NULL, 1.00, {safe_value(row['amount'])}, 0.00, 0.00, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['MedID'])});
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Invoice Details from MedicalReports inserted successfully.")
