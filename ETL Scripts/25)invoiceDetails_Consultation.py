from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

tgt_invoice_df = pd.read_sql('SELECT id as tgt_invoice_id,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL', myconnection)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)

src_consultation = 'SELECT * FROM Consultations'
src_consultation_df = pd.read_sql(src_consultation, get_src_accessdb_connection())
src_consultation_df = src_consultation_df[src_consultation_df['InvoiceNumber'] != 0]
src_consultation_df['InvoiceNumber'] = src_consultation_df['InvoiceNumber'].astype(int)
src_consultation_df = src_consultation_df[['ConsID','InvoiceNumber','ConsultationDate','ConsultationCode','ConsultationCharge','VATRate','VATAmount']]

landing_consultation_df = dd.merge(src_consultation_df, tgt_invoice_df, left_on='InvoiceNumber', right_on='PPM_Invoice_Id', how='inner')
landing_consultation_df = landing_consultation_df.rename(columns={'tgt_invoice_id': 'invoice_id'})
landing_consultation_df = landing_consultation_df.drop(columns=['PPM_Invoice_Id', 'InvoiceNumber'])

src_consultation_trans = 'SELECT * FROM ConsultationTrans'
src_consultation_trans_df = pd.read_sql(src_consultation_trans, get_src_accessdb_connection())
src_consultation_trans_df = src_consultation_trans_df[['ConsTransID','ConsultationDate','InterventionCode','InterventionCharge','VATRate','VATAmount']]
src_consultation_trans_df = src_consultation_trans_df.rename(columns={'ConsTransID': 'ConsID','InterventionCode':'ConsultationCode','InterventionCharge': 'ConsultationCharge'})

#----------------Concating ConsultationTrans with Consultations-------------------
landing_consultation_df2 = pd.concat([landing_consultation_df, src_consultation_trans_df], ignore_index=True)
landing_consultation_df2['invoice_id'] = landing_consultation_df2.groupby('ConsID')['invoice_id'].ffill()

#----------------------invoice date-----------------
def invoiceDate(row):
    if pd.isna(row['ConsultationDate']) or row['ConsultationDate'] == '':
        return None
    else:
        return row['ConsultationDate'].strftime('%Y-%m-%d') 
    
landing_consultation_df2['ConsultationDate'] = landing_consultation_df2.apply(invoiceDate, axis=1)

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoice_details'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
landing_consultation_df2.insert(0, 'invoice_details_id', range(max_id, max_id + len(landing_consultation_df2)))

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoice_details ADD COLUMN IF NOT EXISTS PPM_Invoice_Cons_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#------------------------------Inserting data into invoice_details-----------------------------
tgt_invoice_details_df = pd.read_sql('SELECT DISTINCT PPM_Invoice_Cons_Id FROM invoice_details WHERE PPM_Invoice_Cons_Id IS NOT NULL', myconnection)
landing_consultation_df2['ConsID'] = landing_consultation_df2['ConsID'].astype(str)
tgt_invoice_details_df['PPM_Invoice_Cons_Id'] = tgt_invoice_details_df['PPM_Invoice_Cons_Id'].astype(str)
invoice_consultation_df = landing_consultation_df2[~landing_consultation_df2['ConsID'].isin(tgt_invoice_details_df['PPM_Invoice_Cons_Id'])]

#-----------------------------amount-------------------------------
invoice_consultation_df['amount'] = invoice_consultation_df['ConsultationCharge'] - invoice_consultation_df['VATAmount']

bar = tqdm(total=len(invoice_consultation_df), desc='Inserting Invoice Details from Consultations')

for index,row in invoice_consultation_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoice_details` (id,`invoice_id`, `procedure_date`, `procedure_id`, `procedure_code`, `procedure_name`, `service_location_id`, `qty`, `amount`, `total`, `inv_fee_split_percentage`, `inv_fee_split_amount`, `created_at`, `updated_at`, `deleted_at`,PPM_Invoice_Cons_Id) 
        VALUES ({safe_value(row['invoice_details_id'])},{safe_value(row['invoice_id'])}, {safe_value(row['ConsultationDate'])}, NULL, NULL, {safe_value(row['ConsultationCode'])}, NULL, 1.00, {safe_value(row['amount'])}, 0.00, 0.00, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,{safe_value(row['ConsID'])});
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Invoice Details from Consultations inserted successfully.')