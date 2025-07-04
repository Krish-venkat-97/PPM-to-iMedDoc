from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoices = 'SELECT * FROM InvoiceHeadSummary'
src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb_connection())

#----------------------patient mapping---------------------
src_invoices_df['PatientCode'] = src_invoices_df['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id, PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_invoice_df = dd.merge(src_invoices_df, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#----------------------dropping None patients rows----------------- 
landing_invoice_df = landing_invoice_df[~landing_invoice_df['patient_id'].isna()]  

#----------------------invoice date-----------------
def invoiceDate(row):
    if pd.isna(row['InvoiceDate']):
        return None
    else:
        return row['InvoiceDate'].strftime('%Y-%m-%d') 
    
landing_invoice_df['InvoiceDate'] = landing_invoice_df.apply(invoiceDate, axis=1)



print(landing_invoice_df.columns)