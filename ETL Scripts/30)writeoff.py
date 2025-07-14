import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_payrec = 'SELECT * FROM "Payments Received"'
src_payrec_df = pd.read_sql(src_payrec, get_src_accessdb_connection())

src_writeoff_df = src_payrec_df[src_payrec_df['PaymentMethod'].str.lower() == 'write-off']


tgt_invoice = 'SELECT id as invoice_id,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL'
tgt_invoice_df = pd.read_sql(tgt_invoice, myconnection)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)

#------------------------filtering out the invoice which is not used--------------------
src_writeoff_df1 = dd.merge(src_writeoff_df, tgt_invoice_df, left_on='InvoiceNo', right_on='PPM_Invoice_Id', how='inner')

tgt_payment_types = 'SELECT DISTINCT name as payment_type FROM payment_types'
tgt_payment_types_df = pd.read_sql(tgt_payment_types, myconnection)

other_payment_types_df = pd.DataFrame(['Others','Write-off','Contra'],columns=['payment_types'])

#------------------------inserting payment types if not exists--------------------
for index,row in other_payment_types_df.iterrows():
    if row['payment_types'] not in tgt_payment_types_df['payment_type'].values:
        insert_pay_type = f"""
        INSERT INTO `payment_types` (`name`, `order`, `is_default`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['payment_types'])}, 0, 0, '0000-00-00 00:00:00', '0000-00-00 00:00:00', NULL);
        """
        target_cursor.execute(insert_pay_type)
    else:
        pass
  
myconnection.commit()

src_writeoff_df2 = src_writeoff_df1[['ReceiptNo','invoice_id','PaymentDate','AmountPaid','Spare2']]

#----------------------------payment date-----------------------------
def paymentDate(row):
    if pd.isna(row['PaymentDate']) or row['PaymentDate'] == '':
        return None
    else:
        return row['PaymentDate'].strftime('%Y-%m-%d')

src_writeoff_df2['PaymentDate'] = src_writeoff_df2.apply(paymentDate, axis=1) 

#----------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE write_offs ADD COLUMN IF NOT EXISTS PPM_Receipt_writeoff_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#------------------------------id generation------------------------------
writeoff_max = 'SELECT MAX(id) FROM write_offs'
writeoff_max_df = pd.read_sql(writeoff_max, myconnection)
if writeoff_max_df is None or writeoff_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = writeoff_max_df.iloc[0, 0] + 1
src_writeoff_df2.insert(0, 'writeoff_id', range(max_id, max_id + len(src_writeoff_df2)))

#------------------------------filtering out write-off already exist---------------------
tgt_writeoff = 'SELECT PPM_Receipt_writeoff_Id FROM write_offs WHERE PPM_Receipt_writeoff_Id IS NOT NULL'
tgt_writeoff_df = pd.read_sql(tgt_writeoff, myconnection)
src_writeoff_df2['ReceiptNo'] = src_writeoff_df2['ReceiptNo'].astype(str)
tgt_writeoff_df['PPM_Receipt_writeoff_Id'] = tgt_writeoff_df['PPM_Receipt_writeoff_Id'].astype(str)
src_writeoff_df2 = src_writeoff_df2[~src_writeoff_df2['ReceiptNo'].isin(tgt_writeoff_df['PPM_Receipt_writeoff_Id'])]

#------------------------------inserting into target------------------------------
bar = tqdm(total=len(src_writeoff_df2), desc='Inserting write-off records')

for index, row in src_writeoff_df2.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `write_offs` (id,`date`, `writeoff_memo`, `amount`, `invoice_id`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `PPM_Receipt_writeoff_Id`) 
        VALUES ({safe_value(row['writeoff_id'])},{safe_value(row['PaymentDate'])},{safe_value(row['Spare2'])}, {safe_value(row['AmountPaid'])}, {safe_value(row['invoice_id'])}, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['ReceiptNo'])});
        """
        target_cursor.execute(query)

        # Update the write-off id in the source DataFrame
        update_invoice = f"""
        UPDATE invoices
        SET invoice_writeoff_status = 3
        WHERE id = {safe_value(row['invoice_id'])};
        """
        target_cursor.execute(update_invoice)

    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Write-off records inserted successfully.')