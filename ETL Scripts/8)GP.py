from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_gp = 'SELECT * FROM CodeGPs'
src_gp_df = pd.read_sql(src_gp, get_src_accessdb2_connection())

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS PPM_GP_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#gp_id generation
gp_max = 'SELECT MAX(id) FROM contacts'
gp_max_df = pd.read_sql(gp_max,myconnection)
if gp_max_df is None or gp_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = gp_max_df.iloc[0, 0] + 1
src_gp_df.insert(0,'gp_id',range(max_id,max_id+len(src_gp_df)))

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

gp_df = dd.merge(src_gp_df, tgt_title_df, left_on='GPTitle', right_on='title_name', how='left')
gp_df['title_id'] = gp_df['title_id'].fillna(0).astype(int)

bar = tqdm(total=len(src_gp_df),desc='Inserting GPs',position=0)

tgt_gp_df = pd.read_sql('SELECT DISTINCT PPM_GP_Id FROM contacts', myconnection)

gp_df['display_name'] =  (gp_df['GPInitials'].fillna('') + ' ' + gp_df['GPName']).str.strip()

#--------------------------filtering out GPs already present in target--------------------------
# Convert both columns to the same data type before filtering
gp_df['GPCode'] = gp_df['GPCode'].astype(str)
tgt_gp_df['PPM_GP_Id'] = tgt_gp_df['PPM_GP_Id'].astype(str)
gp_df = gp_df[~gp_df['GPCode'].isin(tgt_gp_df['PPM_GP_Id'])]

for index, row in gp_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `contacts` (id,`contact_type_id`, `title_id`, `first_name`, `sur_name`, `display_name`, `professional_title`, `entity_name`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `work_phone`, `home_phone`, `mobile`, `email`, `website`, `fax`, `is_archive`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_GP_Id) 
        VALUES (
        {row['gp_id']},
        3, 
        {safe_value(row['title_id'])}, 
        {safe_value(row['GPInitials'] if pd.notna(row['GPInitials']) else '')}, 
        {safe_value(row['GPName'])}, 
        {safe_value(row['display_name'])}, 
        NULL, 
        {safe_value(row['GPPractice'])}, 
        {safe_value(row['GPAddress1'])}, 
        {safe_value(row['GPAddress2'])}, 
        {safe_value(row['GPAddress3'])}, 
        {safe_value(row['GPAddress4'])}, 
        {safe_value(row['GPAddress5'])}, 
        {safe_value(row['GPPostCode'])}, 
        {safe_value(row['GPTelNo1'])},
        {safe_value(row['GPTelNo2'])},
        {safe_value(row['GPMobilePhone'])}, 
        NULL, NULL, 
        {safe_value(row['GPFaxNo'])}, 
        0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['GPCode'])}
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break
   

myconnection.commit()
myconnection.close()
bar.close()
print("GPs inserted successfully.")


