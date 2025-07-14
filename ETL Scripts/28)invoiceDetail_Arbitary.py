import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoices = 'SELECT * FROM InvoiceHeadSummary'
src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb_connection())
src_invoices_df = src_invoices_df[src_invoices_df['Invoice Number'] != 0]
src_arbitary_invoice_df = src_invoices_df[src_invoices_df['Txt1']=='AI']

src_arbitary_invoice_df1 = src_arbitary_invoice_df[['Invoice Number','TotalValue','VATAmount','Txt2']]

tgt_invoice = 'SELECT id as invoice_id,invoice_date,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL'
tgt_invoice_df = pd.read_sql(tgt_invoice, myconnection)

tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)
src_arbitary_invoice_df1 = dd.merge(src_arbitary_invoice_df1, tgt_invoice_df, left_on='Invoice Number', right_on='PPM_Invoice_Id', how='inner')
src_arbitary_invoice_df1['amount'] = src_arbitary_invoice_df1['TotalValue'] - src_arbitary_invoice_df1['VATAmount']

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoice_details'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
src_arbitary_invoice_df1.insert(0, 'invoice_details_id', range(max_id, max_id + len(src_arbitary_invoice_df1)))

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoice_details ADD COLUMN IF NOT EXISTS PPM_Invoice_Arbitary_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#-----------------------------filtering out the invoice which is not used--------------------
tgt_invoice_details_df = pd.read_sql('SELECT PPM_Invoice_Arbitary_Id FROM invoice_details WHERE PPM_Invoice_Arbitary_Id IS NOT NULL', myconnection)
tgt_invoice_details_df['PPM_Invoice_Arbitary_Id'] = tgt_invoice_details_df['PPM_Invoice_Arbitary_Id'].astype(str)
src_arbitary_invoice_df1['Invoice Number'] = src_arbitary_invoice_df1['Invoice Number'].astype(str)
invoice_arbitary_df = src_arbitary_invoice_df1[~src_arbitary_invoice_df1['Invoice Number'].isin(tgt_invoice_details_df['PPM_Invoice_Arbitary_Id'].to_list())]

#------------------------------Inserting data into invoice_details-----------------------------
bar = tqdm(total=len(src_arbitary_invoice_df1), desc='Inserting Invoice Details from Arbitary Invoices')

for index, row in invoice_arbitary_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoice_details` (id, `invoice_id`, `procedure_date`, `procedure_id`, `procedure_code`, `procedure_name`, `service_location_id`, `qty`, `amount`, `total`, `inv_fee_split_percentage`, `inv_fee_split_amount`, `created_at`, `updated_at`, `deleted_at`, PPM_Invoice_Arbitary_Id) 
        VALUES ({safe_value(row['invoice_details_id'])}, {safe_value(row['invoice_id'])}, {safe_value(row['invoice_date'])}, NULL, NULL, {safe_value(row['Txt2'])}, NULL, 1.00, {safe_value(row['amount'])}, 0.00, 0.00, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['Invoice Number'])});
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting invoice details for row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Invoice Details from Arbitary Invoices inserted successfully.")

