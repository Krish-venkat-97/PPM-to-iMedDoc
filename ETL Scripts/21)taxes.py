from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoices = 'SELECT * FROM InvoiceHeadSummary'
src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb_connection())

src_receipt = 'SELECT * FROM "Payments Received"'
src_receipt_df = pd.read_sql(src_receipt, get_src_accessdb_connection())

#------------------------invoice tax----------------------------------
src_invoice_tax_df = src_invoices_df[src_invoices_df['VATRate'] != 0]['VATRate'].drop_duplicates().reset_index(drop=True)
src_invoice_tax_df = src_invoice_tax_df.to_frame(name='VATRate')
src_invoice_tax_df = src_invoice_tax_df[src_invoice_tax_df['VATRate'].notna()]
src_invoice_tax_df['VATRate'] = src_invoice_tax_df['VATRate'].astype(float)
src_invoice_tax_df['VATPercent'] = src_invoice_tax_df['VATRate'].apply(lambda x: x*100)
src_invoice_tax_df['VATName'] = src_invoice_tax_df['VATRate'].apply(lambda x: f"{x*100:.2f}% VAT")

#----------------------receipt tax---------------------------------
src_receipt_tax_df = src_receipt_df[src_receipt_df['VATRate'] != 0]['VATRate'].drop_duplicates().reset_index(drop=True)
src_receipt_tax_df = src_receipt_tax_df.to_frame(name='VATRate')
src_receipt_tax_df = src_receipt_tax_df[src_receipt_tax_df['VATRate'].notna()]
src_receipt_tax_df['VATRate'] = src_receipt_tax_df['VATRate'].astype(float)
src_receipt_tax_df['VATPercent'] = src_receipt_tax_df['VATRate'].apply(lambda x: x*100)
src_receipt_tax_df['VATName'] = src_receipt_tax_df['VATRate'].apply(lambda x: f"{x*100:.2f}% VAT")

src_tax_df = pd.concat([src_invoice_tax_df, src_receipt_tax_df], ignore_index=True)

#-------------------id generation-------------------
tax_max = 'SELECT MAX(id) FROM taxes'
tax_max_df = pd.read_sql(tax_max, myconnection)
if tax_max_df is None or tax_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = tax_max_df.iloc[0, 0] + 1
src_tax_df.insert(0, 'tax_id', range(max_id, max_id + len(src_tax_df)))

#----------------------filtyering out rows already present in target database -----------------
existing_tax_ids = pd.read_sql("SELECT perc FROM taxes", myconnection)['perc'].tolist()
src_tax_df = src_tax_df[~src_tax_df['VATPercent'].isin(existing_tax_ids)]

#---------------------inserting taxes into target database---------------------
bar = tqdm(total=len(src_tax_df), desc='Inserting Taxes', position=0)

for index,row in src_tax_df.iterrows():
    bar.update(1)
    try:
        tax_query = f"""
        INSERT INTO `taxes` (`id`, `name`, `perc`, `is_default`, `is_archive`, `tax_type_id`, `system_generated`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['tax_id'])}, {safe_value(row['VATName'])}, {safe_value(row['VATPercent'])}, 1, 1, 0, 1, 0, 0, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL);
        """
        target_cursor.execute(tax_query)

        tax_detail_query = f"""
        INSERT INTO `tax_details` (`tax_id`, `name`, `perc`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['tax_id'])}, {safe_value(row['VATName'])}, {safe_value(row['VATPercent'])}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL);
        """
        target_cursor.execute(tax_detail_query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Tax data inserted successfully.")