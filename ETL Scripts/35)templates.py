import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value,getSourceFilePath, getTargetFilePath, getLogFilePath

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")


src_templates = 'SELECT * FROM LetterData'
try:
    src_templates_df = pd.read_sql(src_templates, get_src_accessdb2_connection())
except:
    src_templates_df = pd.read_sql(src_templates, get_src_accessdb_connection())

def getTemplateFilePath(row):
    if pd.isna(row['Spare1']):
        return None
    else:
        return os.path.join(getSourceFilePath(), 'Templates', row['Spare1'])
    
src_templates_df['source_file_path'] = src_templates_df.apply(getTemplateFilePath, axis=1)

def getExtension(row):
    if pd.isna(row['Spare1']):
        return None
    else:
        return os.path.splitext(row['Spare1'])[1]
src_templates_df['file_extension'] = src_templates_df.apply(getExtension, axis=1)

def fileExists(row):
    if os.path.exists(row['source_file_path']):
        return 1
    else:
        return 0
src_templates_df['file_exists'] = src_templates_df.apply(fileExists, axis=1)

src_templates_df1 = src_templates_df[src_templates_df['file_exists'] == 1]

#------------------------------Adding Source identifier column in target-------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE templates ADD COLUMN IF NOT EXISTS PPM_Template_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()
#------------------------------Template id generation---------------------------------------
template_max = 'SELECT MAX(id) FROM templates'
template_max_df = pd.read_sql(template_max, myconnection)
if template_max_df is None or template_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = template_max_df.iloc[0, 0] + 1
src_templates_df1.insert(0, 'template_id', range(max_id, max_id + len(src_templates_df1)))

def createTemplateFilePath(row):
    if pd.isna(row['Spare1']):
        return None
    else:
        return os.path.join(getTargetFilePath(), 'Templates', (str(row['template_id']) + row['file_extension']))
src_templates_df1['target_file_path'] = src_templates_df1.apply(createTemplateFilePath, axis=1)
src_templates_df1['target_file_path'] = src_templates_df1['target_file_path'].astype(str)

bar = tqdm(total=len(src_templates_df1), desc='Inserting Templates')

#----------------filtering out templates that already exist in target----------------
tgt_templates = 'SELECT PPM_Template_Id FROM templates WHERE PPM_Template_Id IS NOT NULL'
tgt_templates_df = pd.read_sql(tgt_templates, myconnection)
tgt_templates_df['PPM_Template_Id'] = tgt_templates_df['PPM_Template_Id'].astype(str)
src_templates_df1['Spare1'] = src_templates_df1['Spare1'].astype(str)
src_templates_df1 = src_templates_df1[~src_templates_df1['Spare1'].isin(tgt_templates_df['PPM_Template_Id'])]

for index, row in src_templates_df1.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `templates` (`name`, `doctor_id`, `available_editor_id`, `is_default`, `is_archive`, `show`, `ngupload`, `priority_id`, `regenerate`, `status`, `order_by`, `dictate_it_template_code`, `speech_to_text_template_code`, `auto_sendmail`, `category_id`, `is_converted`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, `PPM_Template_Id`) 
        VALUES ({safe_value(row['Invoiced To'])}, 1, 2, NULL, 1, 0, 0, 0, 0, 0, 100, '0', '0', 0, NULL, 0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, {safe_value(row['Spare1'])});
        """
        target_cursor.execute(query)
        # Copy the file to the target location
        if not os.path.exists(os.path.dirname(row['target_file_path'])):
            os.makedirs(os.path.dirname(row['target_file_path']))
        shutil.copy(row['source_file_path'], row['target_file_path'])   

    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Templates inserted successfully.')