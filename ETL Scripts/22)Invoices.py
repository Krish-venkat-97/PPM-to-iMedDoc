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
    if pd.isna(row['Date Created']) or row['Date Created'] == '':
        return None
    else:
        return row['Date Created'].strftime('%Y-%m-%d') 
    
landing_invoice_df['InvoiceDate'] = landing_invoice_df.apply(invoiceDate, axis=1)

#--------------------------only needed columns-------------------
landing_invoice_df1 = landing_invoice_df[['Invoice Number','InvoiceDate','patient_id','TotalValue','VATRate', 'VATAmount','InvoiceTo','AccountName','EDIClaim']]

#--------------------------Mapping tax-------------------
tgt_tax_df = pd.read_sql("SELECT id as tax_id, name as tax_name, perc as tax_perc FROM taxes", myconnection)
tgt_tax_df['tax_perc'] = tgt_tax_df['tax_perc'].astype(float)
landing_invoice_df1['VATRate'] = landing_invoice_df1['VATRate'].fillna(0) # Fill NaN values with 0
landing_invoice_df1['VATRate'] = (landing_invoice_df1['VATRate']*100).astype(float)
landing_invoice_df1 = landing_invoice_df1.merge(tgt_tax_df, left_on='VATRate', right_on='tax_perc', how='left')
landing_invoice_df1.drop(columns=['tax_perc','tax_name'], inplace=True)


print(landing_invoice_df1.columns)