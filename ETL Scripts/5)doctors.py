from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_doctor = 'SELECT * FROM DiaryResources'
src_doctor_df = pd.read_sql(src_doctor, get_src_accessdb_connection())

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE doctors ADD COLUMN IF NOT EXISTS PPM_doctor_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#doctor_id generation
doctor_max = 'SELECT MAX(id) FROM doctors'
doctor_max_df = pd.read_sql(doctor_max,myconnection)
if doctor_max_df is None or doctor_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = doctor_max_df.iloc[0, 0] + 1
src_doctor_df.insert(0,'doctor_id',range(max_id,max_id+len(src_doctor_df)))

#get title_id
src_doctor_df['title'] = src_doctor_df['ResourceName'].apply(lambda x: x.split()[0] if pd.notna(x) else '')

#display_name
src_doctor_df['display_name'] = src_doctor_df['ResourceName'].apply(lambda x: ' '.join(x.split()[1:]) if pd.notna(x) else '')

#surname
src_doctor_df['surname'] = src_doctor_df['ResourceName'].apply(lambda x: x.split()[-1] if pd.notna(x) else '')

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

doctor_df = dd.merge(src_doctor_df, tgt_title_df, left_on='title', right_on='title_name', how='left')
doctor_df['title_id'] = doctor_df['title_id'].fillna(0).astype(int)

tgt_doctor_df = pd.read_sql('SELECT DISTINCT PPM_doctor_Id FROM doctors', myconnection)

bar = tqdm(total=len(src_doctor_df), desc='Inserting doctor', position=0)

for index, row in doctor_df.iterrows():
    bar.update(1)
    if row['ResourceId'] not in tgt_doctor_df['PPM_doctor_Id'].values:
        try:
            query = f"""
            INSERT INTO `doctors` (`id`, `title_id`, `name`, short_name, `created_at`, `updated_at`, created_user_id, updated_user_id, `PPM_doctor_Id`) 
            VALUES (
            {safe_value(row['doctor_id'])}, 
            {safe_value(row['title_id'])}, 
            {safe_value(row['display_name'])},
            {safe_value(row['surname'])}, 
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
            1,1,
            {safe_value(row['ResourceId'])}
            );
            """
            target_cursor.execute(query)
        except Exception as e:
            logging.error(f"Error inserting doctor {row['ResourceId']}: {e}")
            break

myconnection.commit()
myconnection.close()
bar.close()
