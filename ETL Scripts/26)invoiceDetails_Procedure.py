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

src_procedure = 'SELECT * FROM Procedures'
src_procedure_df = pd.read_sql(src_procedure, get_src_accessdb_connection())
src_procedure_df = src_procedure_df[['ProcID','InvoiceNumber']]
src_procedure_df['InvoiceNumber'] = src_procedure_df['InvoiceNumber'].astype(int)

landing_procedure_df = dd.merge(src_procedure_df, tgt_invoice_df, left_on='InvoiceNumber', right_on='PPM_Invoice_Id', how='inner')
landing_procedure_df = landing_procedure_df.rename(columns={'tgt_invoice_id': 'invoice_id'})
landing_procedure_df = landing_procedure_df.drop(columns=['PPM_Invoice_Id','InvoiceNumber'])

src_procedure_trans = 'SELECT * FROM ProcedureTrans'
src_procedure_trans_df = pd.read_sql(src_procedure_trans, get_src_accessdb_connection())
src_procedure_trans_df = src_procedure_trans_df[['ProcTransID','ProcedureDate','ProcedureCode','ProcedureDescription','SurgeonsFee','VATAmount']]
src_procedure_trans_df = src_procedure_trans_df.rename(columns={'ProcTransID': 'ProcID'})


#-----------------------------including the invoicenumber in the src_procedure_trans_df--------------------------
landing_procedure_df['ProcID'] = landing_procedure_df['ProcID'].astype(int)
src_procedure_trans_df['ProcID'] = src_procedure_trans_df['ProcID'].astype(int)
landing_procedure_df2 = dd.merge(landing_procedure_df,src_procedure_trans_df, on='ProcID', how='inner')

#----------------------invoice date-----------------
def invoiceDate(row):
    if pd.isna(row['ProcedureDate']) or row['ProcedureDate'] == '':
        return None
    else:
        return row['ProcedureDate'].strftime('%Y-%m-%d') 
    
landing_procedure_df2['ProcedureDate'] = landing_procedure_df2.apply(invoiceDate, axis=1)

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoice_details'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
landing_procedure_df2.insert(0, 'invoice_details_id', range(max_id, max_id + len(landing_procedure_df2)))

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoice_details ADD COLUMN IF NOT EXISTS PPM_Invoice_Proc_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#------------------------------Inserting data into invoice_details-----------------------------
tgt_invoice_details_df = pd.read_sql('SELECT DISTINCT PPM_Invoice_Proc_Id FROM invoice_details WHERE PPM_Invoice_Proc_Id IS NOT NULL', myconnection)
tgt_invoice_details_df['PPM_Invoice_Proc_Id'] = tgt_invoice_details_df['PPM_Invoice_Proc_Id'].astype(str)
landing_procedure_df2['ProcID'] = landing_procedure_df2['ProcID'].astype(str)
# Filtering out rows already present in target database
invoice_procedure_df = landing_procedure_df2[~landing_procedure_df2['ProcID'].isin(tgt_invoice_details_df['PPM_Invoice_Proc_Id'])]

#-----------------------------amount-------------------------------
invoice_procedure_df['amount'] = invoice_procedure_df['SurgeonsFee'] - invoice_procedure_df['VATAmount']

bar = tqdm(total=len(invoice_procedure_df), desc='Inserting Invoice Details from Procedures')

for index, row in invoice_procedure_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoice_details` (id, `invoice_id`, `procedure_date`, `procedure_id`, `procedure_code`, `procedure_name`, `service_location_id`, `qty`, `amount`, `total`, `inv_fee_split_percentage`, `inv_fee_split_amount`, `created_at`, `updated_at`, `deleted_at`, PPM_Invoice_Proc_Id) 
        VALUES ({safe_value(row['invoice_details_id'])}, {safe_value(row['invoice_id'])}, {safe_value(row['ProcedureDate'])}, NULL, {safe_value(row['ProcedureCode'])}, {safe_value(row['ProcedureDescription'])}, NULL, 1.00, {safe_value(row['amount'])}, 0.00, 0.00, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['ProcID'])});
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Invoice Details from Procedures inserted successfully.")
