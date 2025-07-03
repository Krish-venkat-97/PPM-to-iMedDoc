from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_hospital = 'SELECT * FROM DiaryLocation'
src_hospital_df = pd.read_sql(src_hospital, get_src_accessdb_connection())

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE hospitals ADD COLUMN IF NOT EXISTS PPM_hospital_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#hospital_id generation
hospital_max = 'SELECT MAX(id) FROM hospitals'
hospital_max_df = pd.read_sql(hospital_max,myconnection)
if hospital_max_df is None or hospital_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = hospital_max_df.iloc[0, 0] + 1
src_hospital_df.insert(0,'hospital_id',range(max_id,max_id+len(src_hospital_df)))

tgt_hospital_df = pd.read_sql('SELECT DISTINCT UPPER(LTRIM(RTRIM(name))) as LocationName FROM hospitals', myconnection)
src_hospital_df['LocationName_Upper'] = src_hospital_df['LocationName'].str.upper().str.strip()

#Filtering out the hospitals that already exist in the target
src_hospital_df = src_hospital_df[~src_hospital_df['LocationName_Upper'].isin(tgt_hospital_df['LocationName'].values)]

bar = tqdm(total=len(src_hospital_df), desc='Inserting hospitals', position=0)

for index, row in src_hospital_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `hospitals` (`id`, `name`, type_id, income_category_id, `created_at`, `updated_at`, `PPM_hospital_Id`) 
        VALUES (
        {safe_value(row['hospital_id'])}, 
        {safe_value(row['LocationName'])}, 
        4,1,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        {safe_value(row['LocationId'])}
        )
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting hospital {row['LocationName']}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
