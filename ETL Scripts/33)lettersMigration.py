from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value,getSourceFilePath, getTargetFilePath, getLogFilePath

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_documents = 'SELECT * FROM PatientDocHistory'
src_documents_df = pd.read_sql(src_documents, get_src_accessdb_connection())

def getFileExtension(filename):
    if pd.isna(filename):
        return None
    else:
        return os.path.splitext(filename)[1].lower()
    
src_documents_df['FileExtension'] = src_documents_df['DocFileName'].apply(getFileExtension)

src_letter_df = src_documents_df[src_documents_df['FileExtension'].isin(['.doc', '.docx', '.rtf'])]

#-----------------------------Combining source and target data----------------------
tgt_letter_df = pd.read_sql("SELECT id as letter_id, patient_id, PPM_Letter_Id FROM letters WHERE PPM_Letter_Id IS NOT NULL", myconnection)
tgt_letter_df['PPM_Letter_Id'] = tgt_letter_df['PPM_Letter_Id'].astype(int)
src_letter_df['ID'] = src_letter_df['ID'].astype(int)
landing_letter_df = dd.merge(src_letter_df, tgt_letter_df, left_on='ID', right_on='PPM_Letter_Id', how='inner')

#-----------------------------filterinig out the rows with SubDirectory = None--------------
landing_letter_df = landing_letter_df[~landing_letter_df['SubDirectory'].isna()] 

#------------------------getting source file location-----------------------
def getSourceFileLocation(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(getSourceFilePath(), row['SubDirectory'], row['DocFileName'])

landing_letter_df['SourceFileLocation'] = landing_letter_df.apply(getSourceFileLocation, axis=1)

#----------------------------getting target file directory-----------
def getTargetFileDirectory(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(getTargetFilePath(), 'patients', str(row['patient_id']), 'letters')
    
landing_letter_df['TargetFileDirectory'] = landing_letter_df.apply(getTargetFileDirectory, axis=1)

#--------------------------------getting target file location-------------------
def getTargetFileLocation(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(row['TargetFileDirectory'], (str(row['letter_id'])+row['FileExtension']))
    
landing_letter_df['TargetFileLocation'] = landing_letter_df.apply(getTargetFileLocation, axis=1)

#----------------------------checking the file exist-------------------
def fileCheck(row):
    if os.path.exists(row['SourceFileLocation']):
        return 1
    else:
        return 0
landing_letter_df['file_check'] = landing_letter_df.apply(fileCheck, axis=1)

#-------------------------------dropping unneeded columns----------------------
landing_letter_df1 = landing_letter_df[['ID','letter_id','FileExtension','SubDirectory','DocFileName','SourceFileLocation','TargetFileDirectory','TargetFileLocation','file_check']]
landing_letter_df2 = landing_letter_df1[landing_letter_df1['file_check']==1]

#--------------------------------Migrating the letters---------------------------
bar = tqdm(total = len(landing_letter_df2), desc='Migrating letters')

missing_source_document = []
missing_source_id = []

for index,row in landing_letter_df2.iterrows():
    bar.update(1)
    try:
        is_file_dffolder_exist = os.path.exists(row['TargetFileDirectory'])
        if not is_file_dffolder_exist:
            os.makedirs(row['TargetFileDirectory'])
            is_file_exist = os.path.exists(row['TargetFileLocation'])
            if not is_file_exist:
                shutil.copy(row['SourceFileLocation'],row['TargetFileLocation'])
            else:
                pass
        else:
            is_file_exist = os.path.exists(row['TargetFileLocation'])
            if not is_file_exist:
                shutil.copy(row['SourceFileLocation'],row['TargetFileLocation'])
            else:
                pass
    except:
        logging.error(f"Error copying file from {row['SourceFileLocation']} to {row['TargetFileLocation']}")
        missing_source_document.append(row['SourceFileLocation'])
        missing_source_id.append(row['ID'])
        

#-----------------------Making a excel file which has missing documents-----------------
if missing_source_document:
    missing_df = pd.DataFrame(zip(missing_source_id,missing_source_document), columns=['Missing Source Letter ID','Missing Source Letter Documents'])
    missing_df.to_excel(os.path.join(getLogFilePath(), 'missing_source_documents.xlsx'), index=False)
    print(f"Missing source documents logged in {getLogFilePath()}/missing_source_letter_documents.xlsx")

print('Letter migration completed successfully!')
bar.close()


