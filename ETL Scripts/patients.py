from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_patient = 'SELECT * FROM CodePatients'
src_patient_df = pd.read_sql(src_patient, get_src_accessdb_connection())

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE patients ADD COLUMN IF NOT EXISTS PPM_Patient_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

# patient_id generation
patient_max = 'SELECT MAX(id) FROM patients'
patient_max_df = pd.read_sql(patient_max, myconnection)
if patient_max_df is None or patient_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = patient_max_df.iloc[0, 0] + 1
src_patient_df.insert(0, 'patient_id', range(max_id, max_id + len(src_patient_df)))

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

src_patient_df['Title_Upper'] = src_patient_df['Title'].str.upper().str.strip()
tgt_title_df['title_name_Upper'] = tgt_title_df['title_name'].str.upper().str.strip()

patient_df = dd.merge(src_patient_df, tgt_title_df, left_on='Title_Upper', right_on='title_name_Upper', how='left')
patient_df['title_id'] = patient_df['title_id'].fillna(0).astype(int)

tgt_insurance_companies = 'SELECT DISTINCT PPM_InsComp_Id, name FROM insurance_companies'
tgt_insurance_companies_df = pd.read_sql(tgt_insurance_companies, myconnection)

tgt_insurance_companies = 'SELECT i.id AS insurance_comp_id,i.name AS insuranc_comp_name FROM insurance_companies i'
tgt_insurance_companies_df = pd.read_sql(tgt_insurance_companies, myconnection)

patient_df['InsuranceCompany_Upper'] = patient_df['InsuranceCompany'].str.upper().str.strip()
tgt_insurance_companies_df['InsuranceCompany_Upper'] = tgt_insurance_companies_df['insuranc_comp_name'].str.upper().str.strip()

patient_df1 = dd.merge(patient_df, tgt_insurance_companies_df, left_on='InsuranceCompany_Upper', right_on='InsuranceCompany_Upper', how='left')
patient_df1['insurance_comp_id'] = patient_df1['insurance_comp_id'].apply(lambda x: int(x) if pd.notnull(x) else None)

tgt_insurance_companies2 = 'SELECT i.id AS insurance_comp_id2,i.name AS insuranc_comp_name2 FROM insurance_companies i'
tgt_insurance_companies_df2 = pd.read_sql(tgt_insurance_companies2, myconnection)

patient_df1['InsuranceCompany_Upper2'] = patient_df1['InsuranceCompany1'].str.upper().str.strip()
tgt_insurance_companies_df2['InsuranceCompany_Upper2'] = tgt_insurance_companies_df2['insuranc_comp_name2'].str.upper().str.strip()

patient_df2 = dd.merge(patient_df1, tgt_insurance_companies_df2, left_on='InsuranceCompany_Upper2', right_on='InsuranceCompany_Upper2', how='left')
patient_df2['insurance_comp_id2'] = patient_df2['insurance_comp_id2'].apply(lambda x: int(x) if pd.notnull(x) else None)

patient_df2['gender'] = patient_df2.apply(lambda row: 1 if row['Sex']==False else 2, axis=1)

bar = tqdm(total=len(src_patient_df), desc='Inserting Patients', position=0)

tgt_patient_df = pd.read_sql('SELECT DISTINCT PPM_Patient_Id FROM patients', myconnection)

patient_df3 = patient_df2[~patient_df1['PatientCode'].isin(tgt_patient_df['PPM_Patient_Id'])]

def displayName(row):
    first_name = row['FirstNames'].strip() if pd.notna(row['FirstNames']) else ''
    sur_name = row['LastName'].strip() if pd.notna(row['LastName']) else ''
    return f"{first_name} {sur_name}".strip()

patient_df3['display_name'] = patient_df3.apply(displayName, axis=1)

def surDisplayName(row):
    first_name = row['FirstNames'].strip() if pd.notna(row['FirstNames']) else ''
    sur_name = row['LastName'].strip() if pd.notna(row['LastName']) else ''
    return f"{sur_name} {first_name}".strip()

patient_df3['sur_display_name'] = patient_df3.apply(surDisplayName, axis=1)

for index,row in patient_df3.iterrows():
    bar.update(1)
    try:
        sql1 = f"""
        INSERT INTO patients (id,doctor_id,shared_doctor,title_id,first_name,surname,display_name,display_first_sur_name,display_sur_first_name,dob,address1,address2,address3,address4,county,postcode,home_phone,work_phone,mobile,emails,occupation,gender,patient_type_id,primary_insurance_company_id,secondary_insurance_company_id,primary_insurance_no,secondary_insurance_no,notes,created_at,updated_at,created_user_id,updated_user_id,PPM_Patient_Id)
            VALUES (
            {safe_value(row['patient_id'])},
            1,1,
            {safe_value(row['title_id'])},
            {safe_value(row['FirstNames'])},
            {safe_value(row['LastName'])},
            {safe_value(row['display_name'])},
            {safe_value(row['display_name'])},
            {safe_value(row['sur_display_name'])},
            {safe_value(row['Birthdate'])},
            {safe_value(row['Address1'])},
            {safe_value(row['Address2'])},
	        {safe_value(row['Address3'])},
            {safe_value(row['TownorCity'])},
            {safe_value(row['County'])},
            {safe_value(row['PostalCode'])},
            {safe_value(row['HomePhone'])},
	        {safe_value(row['WorkPhone'])},
            {safe_value(row['MobilePhone'])},
            NULL,
            {safe_value(row['Occupation'])},
            {safe_value(row['gender'])},
            1,
            {safe_value(row['insurance_comp_id'])},
	        {safe_value(row['insurance_comp_id2'])},
	        {safe_value(row['InsurancePolicyNo'])},
	        {safe_value(row['InsurancePolicyNo1'])},
	        {safe_value(row['PatientNotes'])},
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            1,
            1,
            {safe_value(row['PatientCode'])}
            );
        """
        target_cursor.execute(sql1)
        #Inserting personal_histories
        sql2 = f"""
        INSERT INTO personal_histories (patient_id,created_at,updated_at,created_user_id,updated_user_id)
        values (
        {safe_value(row['patient_id'])},
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1,
        1
        )
        """
        target_cursor.execute(sql2)
        #Inserting medical_histories
        sql3 = f"""
        INSERT INTO medical_histories (patient_id,created_at,updated_at,created_user_id,updated_user_id)
        values (
        {safe_value(row['patient_id'])},
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1,
        1
        )
        """
        target_cursor.execute(sql3)
        #Inserting First visit
        sql4 = f"""
        INSERT INTO episodes (patient_id,name,description,start_date,active,is_general,created_at,updated_at,created_user_id,updated_user_id)
        values (
        {safe_value(row['patient_id'])},
        'First Visit',
        'First Visit',
        {safe_value(row['FirstVisit'])},
        1,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1,
        1
        )
        """
        target_cursor.execute(sql4)
        #Inserting episodes 
        sql5 = f"""
        INSERT INTO episodes (patient_id,name,description,start_date,active,is_general,created_at,updated_at,created_user_id,updated_user_id)
        values (
        {safe_value(row['patient_id'])},
        'General',
        'General',
        CURDATE(),
        1,
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1,
        1
        )
        """
        target_cursor.execute(sql5)
        
    except Exception as e:
        print(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Patients inserted successfully from CodePatients.")