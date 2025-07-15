import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")


src_insurance_companies  = 'SELECT * FROM CodeInsuranceCompanys'

try:
    src_insurance_companies_df = pd.read_sql(src_insurance_companies, get_src_accessdb_connection())
except:
    src_insurance_companies_df = pd.read_sql(src_insurance_companies, get_src_accessdb2_connection())


src_insurance_companies_df['InsCoName_Upper'] = src_insurance_companies_df['InsCoName'].str.upper().str.strip()
src_insurance_companies_df = src_insurance_companies_df.drop_duplicates(subset=['InsCoName_Upper']).reset_index(drop=True)

#
tgt_insurance_companies = 'SELECT DISTINCT name,UPPER(LTRIM(RTRIM(name))) as InsCoName_Upper,UPPER(name) as InsuranceCompany_Upper FROM insurance_companies'
tgt_insurance_companies_df = pd.read_sql(tgt_insurance_companies, myconnection)

# Filtering out insurance companies that already exist in the target
src_insurance_companies_dfx = src_insurance_companies_df[~src_insurance_companies_df['InsCoName_Upper'].isin(tgt_insurance_companies_df['InsuranceCompany_Upper'])]

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE insurance_companies ADD COLUMN IF NOT EXISTS PPM_InsComp_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#insurance_id generation
insurance_max = 'SELECT MAX(id) FROM insurance_companies'
insurance_max_df = pd.read_sql(insurance_max,myconnection)
if insurance_max_df is None or insurance_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = insurance_max_df.iloc[0, 0] + 1
src_insurance_companies_dfx.insert(0,'insurance_comp_id',range(max_id,max_id+len(src_insurance_companies_dfx)))

bar = tqdm(total=len(src_insurance_companies_dfx),desc='Inserting Insurance Companies from CodeInsuranceCompanys',position=0)

for index,row in src_insurance_companies_dfx.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `insurance_companies` (id,`name`, `contact_person`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `phone`, `mobile`, `fax`, `email`, `website`, `form_template`, `form_template_id`, `is_archive`, `is_default`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_InsComp_Id) 
        VALUES (
        {safe_value(row['insurance_comp_id'])},
        {safe_value(row['InsCoName'])}, 
        {safe_value(row['InsCoContactName1'])}, 
        {safe_value(row['InsCoAddress1'])}, 
        {safe_value(row['InsCoAddress2'])}, 
        {safe_value(row['InsCoAddress3'])}, 
        {safe_value(row['InsCoAddress4'])}, 
        {safe_value(row['InsCoAddress5'])}, 
        {safe_value(row['InsCoPostCode'])}, 
        {safe_value(row['InsCoTelNo1'])},
        {safe_value(row['InsCoTelNo2'])},
        {safe_value(row['InsCoFaxNo'])},
        NULL, NULL, NULL, NULL, 0, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['InsuranceCoCode'])}
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break
        
myconnection.commit()
myconnection.close()
bar.close()
print('Insurance Companies data inserted successfully.')
