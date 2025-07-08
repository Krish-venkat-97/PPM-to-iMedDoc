from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoice_hospitals  = 'SELECT * FROM InvoiceHeadSummary WHERE InvoiceTo = 3'
src_invoice_hospitals_df = pd.read_sql(src_invoice_hospitals, get_src_accessdb_connection())


src_invoice_hospitals_df1 = src_invoice_hospitals_df[['AccountName','Hospital','EDIHospitalNumber']]

edi_hospitals_df = src_invoice_hospitals_df1.dropna(subset=['EDIHospitalNumber']).drop_duplicates(subset=['EDIHospitalNumber']).reset_index(drop=True).drop(columns=['Hospital'])

src_invoice_hospitals_df2 = src_invoice_hospitals_df1.merge(edi_hospitals_df, on='EDIHospitalNumber', how='left')

def changeHospital(row):
    account_name = str(row['AccountName_x']).lower()
    hospital_name = str(row['Hospital']).lower()
    secondary_account_name = str(row['AccountName_y']).lower()

    if 'hospital' not in account_name and 'hospital' in secondary_account_name:
        return secondary_account_name
    elif 'hospital' not in account_name and 'hospital' not in secondary_account_name and 'hospital' in hospital_name:
        return hospital_name
    else:
        return account_name
    
src_invoice_hospitals_df2['AccountName_original'] = src_invoice_hospitals_df2.apply(changeHospital, axis=1)

landing_invoice_hospital_df = src_invoice_hospitals_df2[['AccountName_original']]
landing_invoice_hospital_df['AccountName_Upper'] = landing_invoice_hospital_df['AccountName_original'].str.upper().str.strip()
landing_invoice_hospital_df = landing_invoice_hospital_df.drop_duplicates(subset=['AccountName_Upper']).reset_index(drop=True)

tgt_hospitals = pd.read_sql('SELECT DISTINCT name,UPPER(LTRIM(RTRIM(name))) as AccountName_Upper FROM hospitals', myconnection)

# Filtering out hospitals that already exist in the target
src_invoice_hospital_dfx = landing_invoice_hospital_df[~landing_invoice_hospital_df['AccountName_Upper'].isin(tgt_hospitals['AccountName_Upper'])]
src_invoice_hospital_dfx = src_invoice_hospital_dfx.drop(columns=['AccountName_Upper']).reset_index(drop=True)
src_invoice_hospital_dfx = src_invoice_hospital_dfx.rename(columns={'AccountName_original': 'AccountName'})

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE hospitals ADD COLUMN IF NOT EXISTS PPM_InvoiceHospital_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

# Hospital ID generation
hospital_max = 'SELECT MAX(id) FROM hospitals'
hospital_max_df = pd.read_sql(hospital_max, myconnection)
if hospital_max_df is None or hospital_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = hospital_max_df.iloc[0, 0] + 1
src_invoice_hospital_dfx.insert(0, 'hospital_id', range(max_id, max_id + len(src_invoice_hospital_dfx)))

bar = tqdm(total=len(src_invoice_hospital_dfx), desc='Inserting Hospitals from InvoiceHeadSummary')

for index, row in src_invoice_hospital_dfx.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `hospitals` (`id`, `name`, `type_id`, `income_category_id`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `phone`, `mobile`, `fax`, `email`, `website`, `latitude`, `longitude`, `color_code`, `form_template`, `is_default_clinic`, `is_default_hospital`, `is_archive`, `health_claim_hospital_code`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `PPM_InvoiceHospital_Id`) 
        VALUES ({safe_value(row['hospital_id'])}, {safe_value(row['AccountName'].title())}, 4, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '#000000', 0, 0, 0, 1, NULL, 0, 0, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, 'From_InvoiceHeadSummary');
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Hospitals from InvoiceHeadSummary inserted successfully.')
