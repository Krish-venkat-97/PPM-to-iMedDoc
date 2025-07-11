from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

#---------------Mapping 'professional service' to invoice_detial desc where there is no desc in source-----------
tgt_invoice = """
SELECT i.id AS invoice_id,PPM_Invoice_Id,i.invoice_date,'Professional Services' AS descriptions ,i.grand_total AS amount
FROM invoices i
LEFT JOIN invoice_details id
ON i.id = id.invoice_id
WHERE id.invoice_id IS NULL;
"""
tgt_invoice_df = pd.read_sql(tgt_invoice, myconnection)

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoice_details'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
tgt_invoice_df.insert(0, 'invoice_details_id', range(max_id, max_id + len(tgt_invoice_df)))

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoice_details ADD COLUMN IF NOT EXISTS PPM_Invoice_Other_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#---------------------------------filtering out the invoice which is not used--------------------
tgt_invoice_details_df = pd.read_sql('SELECT DISTINCT PPM_Invoice_Other_Id FROM invoice_details WHERE PPM_Invoice_Other_Id IS NOT NULL', myconnection)
tgt_invoice_details_df['PPM_Invoice_Other_Id'] = tgt_invoice_details_df['PPM_Invoice_Other_Id'].astype(str)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(str)
# Filtering out rows already present in target database
invoice_other_df = tgt_invoice_df[~tgt_invoice_df['PPM_Invoice_Id'].isin(tgt_invoice_details_df['PPM_Invoice_Other_Id'].to_list())]

#------------------------------Inserting data into invoice_details-----------------------------
bar = tqdm(total=len(tgt_invoice_df), desc='Inserting Invoice Details from Other Invoices')

for index, row in tgt_invoice_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoice_details` (id, `invoice_id`, `procedure_date`, `procedure_id`, `procedure_code`, `procedure_name`, `service_location_id`, `qty`, `amount`, `total`, `inv_fee_split_percentage`, `inv_fee_split_amount`, `created_at`, `updated_at`, `deleted_at`, PPM_Invoice_Other_Id) 
        VALUES ({safe_value(row['invoice_details_id'])}, {safe_value(row['invoice_id'])}, {safe_value(row['invoice_date'])}, NULL, NULL, {safe_value(row['descriptions'])}, NULL, 1.00, {safe_value(row['amount'])}, 0.00, 0.00, 0.00, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['PPM_Invoice_Id'])});
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting invoice details for row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Invoice Details from Other Invoices inserted successfully.")
