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

src_credit_df = src_payrec_df[src_payrec_df['PaymentMethod'].str.lower() == 'credit']
src_credit_df1 = src_credit_df[['InvoiceNo', 'ReceiptNo','PaymentDate','AmountPaid','Spare2']]

#----------------------------payment date-----------------------------
def paymentDate(row):
    if pd.isna(row['PaymentDate']) or row['PaymentDate'] == '':
        return None
    else:
        return row['PaymentDate'].strftime('%Y-%m-%d')

src_credit_df1['PaymentDate'] = src_credit_df1.apply(paymentDate, axis=1) 

#----------------------------Mapping InvoiceNo to invoice_id-----------------------------
tgt_invoice = 'SELECT id as invoice_id,PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL'
tgt_invoice_df = pd.read_sql(tgt_invoice, myconnection)
tgt_invoice_df['PPM_Invoice_Id'] = tgt_invoice_df['PPM_Invoice_Id'].astype(int)

#------------------------filtering out the invoice which is not used--------------------
src_credit_df1 = pd.merge(src_credit_df1, tgt_invoice_df, left_on='InvoiceNo', right_on='PPM_Invoice_Id', how='inner')
src_credit_df2 = src_credit_df1[['ReceiptNo','invoice_id','PaymentDate','AmountPaid','Spare2']]
"""
landing_credit = 
SELECT invoice_id,SUM(AmountPaid) AS AmountPaid,MAX(PaymentDate) AS PaymentDate,MAX(ReceiptNo) AS ReceiptNo,
GROUP_CONCAT(DISTINCT Spare2) AS Spare2
FROM src_credit_df2
GROUP BY invoice_id
landing_credit_df = ps.sqldf(landing_credit)
"""
landing_credit_df = (
    src_credit_df2
    .groupby('invoice_id', as_index=False)
    .agg({
        'AmountPaid': 'sum',
        'PaymentDate': 'max',
        'ReceiptNo': 'max',
        'Spare2': lambda x: ','.join(sorted(set(str(i) for i in x if pd.notnull(i))))
    })
)

#----------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS PPM_Receipt_Credit_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#-----------------------------filtering out the invoices which already have credit-----------------------------
tgt_invoice_credit = 'SELECT PPM_Receipt_Credit_Id FROM invoices WHERE PPM_Receipt_Credit_Id IS NOT NULL'
tgt_invoice_credit_df = pd.read_sql(tgt_invoice_credit, myconnection)
tgt_invoice_credit_df['PPM_Receipt_Credit_Id'] = tgt_invoice_credit_df['PPM_Receipt_Credit_Id'].astype(str)
landing_credit_df['ReceiptNo'] = landing_credit_df['ReceiptNo'].astype(str) 
landing_credit_df = landing_credit_df[~landing_credit_df['ReceiptNo'].isin(tgt_invoice_credit_df['PPM_Receipt_Credit_Id'])]

#-------------------------------updating the invoice with credit-----------------
bar = tqdm(total=len(landing_credit_df), desc="Updating invoices with credit")

for index, row in landing_credit_df.iterrows():
    bar.update(1)
    try:
        update_query = f"""
        UPDATE invoices i
        SET i.invoice_credit_amount = {safe_value(row['AmountPaid'])},
        i.invoice_credit_note = {safe_value(row['Spare2'])},
        i.invoice_credit_status = 0,
        i.net_total = i.net_total - {safe_value(row['AmountPaid'])},
        i.PPM_Receipt_Credit_Id = {safe_value(row['ReceiptNo'])}
        WHERE i.id = {safe_value(row['invoice_id'])}
        """
        target_cursor.execute(update_query)
    except Exception as e:
        logging.error(f"Error updating row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Credit records updated successfully.')