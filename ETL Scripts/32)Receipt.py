import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_payrec = 'SELECT * FROM "Payments Received"'
try:
    src_payrec_df = pd.read_sql(src_payrec, get_src_accessdb_connection())
except:
    src_payrec_df = pd.read_sql(src_payrec, get_src_accessdb2_connection())

src_payrec_df1 = src_payrec_df[~src_payrec_df['PaymentMethod'].str.lower().isin(['write-off', 'credit'])]
src_payrec_df2 = src_payrec_df1[['InvoiceNo', 'ReceiptNo','PaymentDate','PaymentMethod','AmountPaid','Balance', 'PreviousBalance','Spare2','VATRate','VATAmount']]

tgt_invoice = 'SELECT id as invoice_id,patient_id,billto_id,income_category_id,insurance_company_id,contact_id,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL'
tgt_invoice_df = pd.read_sql(tgt_invoice, myconnection)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)

#------------------------filtering out the invoice which is not used--------------------
src_payrec_df3 = pd.merge(src_payrec_df2, tgt_invoice_df, left_on='InvoiceNo', right_on='PPM_Invoice_Id', how='inner')
src_payrec_df3 = src_payrec_df3.drop(columns=['InvoiceNo', 'PPM_Invoice_Id'])

tgt_payment_types = 'SELECT id as payment_type_id, name as payment_type FROM payment_types'
tgt_payment_types_df = pd.read_sql(tgt_payment_types, myconnection)

#-----------------------------payment type mapping---------------------------
def paymentType(row):
    # Handle None/null values
    if pd.isna(row['PaymentMethod']) or row['PaymentMethod'] is None:
        return 'Others'
    payment_method = row['PaymentMethod'].lower()

    if 'cash' in row['PaymentMethod'].lower():
        return 'Cash'
    elif 'credit' in row['PaymentMethod'].lower():
        return 'Credit card'
    elif 'cheque' in row['PaymentMethod'].lower():
        return 'Cheque'
    elif 'contra' in row['PaymentMethod'].lower():
        return 'Contra'
    else:
        return 'Others'

src_payrec_df3['PaymentMethod'] = src_payrec_df3.apply(paymentType, axis=1)

src_payrec_df4 = pd.merge(src_payrec_df3, tgt_payment_types_df, left_on='PaymentMethod', right_on='payment_type', how='left')

#----------------------------payment date-----------------------------
def paymentDate(row):
    if pd.isna(row['PaymentDate']) or row['PaymentDate'] == '':
        return None
    else:
        return row['PaymentDate'].strftime('%Y-%m-%d')
src_payrec_df4['PaymentDate'] = src_payrec_df4.apply(paymentDate, axis=1)

#----------------------------tax_id mapping-----------------------------
tgt_tax_df = pd.read_sql("SELECT id as tax_id, name as tax_name, perc as tax_perc FROM taxes", myconnection)
tgt_tax_df['tax_perc'] = tgt_tax_df['tax_perc'].astype(float)
src_payrec_df4['VATRate'] = src_payrec_df4['VATRate'].fillna(0) # Fill NaN values with 0
src_payrec_df4['VATRate'] = (src_payrec_df4['VATRate']*100).astype(float)
src_payrec_df5 = src_payrec_df4.merge(tgt_tax_df, left_on='VATRate', right_on='tax_perc', how='left')
src_payrec_df5.drop(columns=['tax_perc','tax_name'], inplace=True)

#----------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE receipts ADD COLUMN IF NOT EXISTS PPM_Receipt_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

src_payrec_df5 = src_payrec_df5.sort_values(by='PaymentDate')

#------------------------------id generation------------------------------
receipt_max = 'SELECT MAX(id) FROM receipts'
receipt_max_df = pd.read_sql(receipt_max, myconnection)
if receipt_max_df is None or receipt_max_df.iloc[0, 0] is None:
    receipt_max_id = 0
else:
    receipt_max_id = receipt_max_df.iloc[0, 0] + 1
src_payrec_df5.insert(0, 'receipt_id', range(receipt_max_id, receipt_max_id + len(src_payrec_df5)))

#-----------------------------receipt details id generation-----------------
receipt_detail_max = 'SELECT MAX(id) FROM receipt_details'
receipt_detail_max_df = pd.read_sql(receipt_detail_max, myconnection)
if receipt_detail_max_df is None or receipt_detail_max_df.iloc[0, 0] is None:
    receipt_detail_max_id = 0
else:
    receipt_detail_max_id = receipt_detail_max_df.iloc[0, 0] + 1
src_payrec_df5.insert(0, 'receipt_detail_id', range(receipt_detail_max_id, receipt_detail_max_id + len(src_payrec_df5)))

#------------------------------filtering out the receipt already exist---------------------
tgt_receipt = 'SELECT PPM_Receipt_Id FROM receipts WHERE PPM_Receipt_Id IS NOT NULL'
tgt_receipt_df = pd.read_sql(tgt_receipt, myconnection)
tgt_receipt_df['PPM_Receipt_Id'] = tgt_receipt_df['PPM_Receipt_Id'].astype(str)
src_payrec_df5['ReceiptNo'] = src_payrec_df5['ReceiptNo'].astype(str)
# Filtering out rows already present in target database
src_payrec_df5 = src_payrec_df5[~src_payrec_df5['ReceiptNo'].isin(tgt_receipt_df['PPM_Receipt_Id'])]

src_payrec_df6 = src_payrec_df5[['receipt_id', 'receipt_detail_id', 'invoice_id', 'patient_id', 'billto_id', 'income_category_id', 'insurance_company_id', 'contact_id', 'PaymentDate', 'payment_type_id', 'tax_id', 'VATAmount', 'AmountPaid', 'Balance', 'PreviousBalance', 'Spare2','ReceiptNo']]
#src_payrec_df6 = src_payrec_df6.sort_values(by='invoice_id')

#------------------------------inserting receipt details---------------------
bar = tqdm(total=len(src_payrec_df5), desc="Inserting receipt details")

for index, row in src_payrec_df5.iterrows():
    bar.update(1)
    try:
        receipt_query = f"""
        INSERT INTO `receipts` (id,`receipt_no`, `receipt_date`, `type_id`, `doctor_id`, `tax_id`, `payment_type_id`, `patient_id`, `patient_address`, `contact_id`, `contact_address`, `income_category_id`, `insurance_company_id`, `cheque_date`, `cheque_no`, `bank_name`, `card_expiry_date`, `card_type_id`, `card_name`, `card_no`, `reference_no`, `amount`, `refund_amount`, `bank_transfer_total_amount`, `bank_transfer_unallocated_amount`, `appointment_id`, `surgery_id`, `is_deleted`, `patient_alt_billing`, `is_deposit_payment`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `PPM_Receipt_Id`) 
        VALUES (
        {safe_value(row['receipt_id'])},
        {safe_value(row['receipt_id'])},
        {safe_value(row['PaymentDate'])}, 
        {safe_value(row['billto_id'])}, 
        1, 
        {safe_value(row['tax_id'])}, 
        {safe_value(row['payment_type_id'])}, 
        {safe_value(row['patient_id'])}, 
        NULL, NULL, 
        {safe_value(row['contact_id'])},
        1, 
        {safe_value(row['insurance_company_id'])}, 
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
        {safe_value(row['AmountPaid'])}, 
        0.00, 0.00, 0.00, NULL, NULL, 0, NULL, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, 
        {safe_value(row['ReceiptNo'])}
        );
        """
        target_cursor.execute(receipt_query)

        receipt_detail_query = f"""
        INSERT INTO `receipt_details` (id,`receipt_id`, `invoice_id`, `balance_amount`, `tax_amount`, `waived_amount`, `net_amount`, `payment`, `accepted_payment`, `refund_amount`, `notes`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES (
        {safe_value(row['receipt_detail_id'])},
        {safe_value(row['receipt_id'])},
        {safe_value(row['invoice_id'])}, 
        {safe_value(row['PreviousBalance'])}, 
        {safe_value(row['VATAmount'])}, 
        0.00, 
        {safe_value(row['PreviousBalance'])}, 
        {safe_value(row['AmountPaid'])}, 
        {safe_value(row['PreviousBalance'])}, 
        0.00, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL
        );
        """
        target_cursor.execute(receipt_detail_query)
    
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Receipt data inserted successfully.')

