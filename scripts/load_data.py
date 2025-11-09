import pandas as pd
import pyodbc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
CSV_FILE_PATH = r'C:\Users\OMEN\Desktop\Learn\FPT\Project_1\data\processed\final_data.csv'
SERVER_NAME = r'DESKTOP-C9TF579'
DATABASE_NAME = 'HeartDisease'
DRIVER = '{ODBC Driver 17 for SQL Server}'

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the SQL Server database."""
    conn_str = (
        f"DRIVER={DRIVER};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        logging.info("Database connection successful.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Database connection failed: {sqlstate}")
        return None

# --- Data Loading Functions ---

def load_dimension(conn, df, table_name, column_map, unique_cols, id_column_name):
    """
    Loads data into a dimension table, avoiding duplicates.
    Returns a dictionary mapping unique column tuples to their new IDs.
    """
    cursor = conn.cursor()
    
    # Get existing data from the dimension table to avoid duplicates
    existing_data = {}
    try:
        cursor.execute(f"SELECT {', '.join(unique_cols)}, {id_column_name} FROM dbo.{table_name}")
        for row in cursor.fetchall():
            key = tuple(row[:-1])
            existing_data[key] = row[-1]
        logging.info(f"Found {len(existing_data)} existing records in {table_name}.")
    except pyodbc.ProgrammingError:
        logging.info(f"Table {table_name} not found or no existing data, proceeding with fresh insert.")
        # Table might not exist on first run, which is okay
        pass

    # Prepare data for insertion
    data_to_insert = df[list(column_map.keys())].drop_duplicates()
    
    id_map = existing_data.copy()
    new_rows = 0

    for _, row in data_to_insert.iterrows():
        key = tuple(row[col] for col in column_map.keys())
        
        if key not in existing_data:
            try:
                placeholders = ', '.join(['?' for _ in column_map.values()])
                sql = f"INSERT INTO dbo.{table_name} ({', '.join(column_map.values())}) OUTPUT INSERTED.{id_column_name} VALUES ({placeholders})"
                
                values = [row[col] for col in column_map.keys()]
                
                cursor.execute(sql, *values)
                new_id = cursor.fetchone()[0]
                id_map[key] = new_id
                new_rows += 1
            except pyodbc.IntegrityError:
                logging.warning(f"Integrity error (possible race condition) inserting into {table_name} for key {key}. Attempting to retrieve existing ID.")
                conn.rollback()
                cursor.execute(f"SELECT {id_column_name} FROM dbo.{table_name} WHERE {' AND '.join([f'{col}=?' for col in unique_cols])}", *key)
                result = cursor.fetchone()
                if result:
                    id_map[key] = result[0]
                else:
                    logging.error(f"Failed to retrieve existing ID after integrity error for {table_name} with key {key}.")
            except Exception as e:
                logging.error(f"Error inserting into {table_name}: {e}. Problematic row: {row.to_dict()}")
                conn.rollback()


    if new_rows > 0:
        conn.commit()
        logging.info(f"Inserted {new_rows} new records into {table_name}.")
    else:
        logging.info(f"No new records to insert into {table_name}.")

    return id_map

def load_fact_table(conn, df, maps):
    """Loads data into the HealthRecord fact table."""
    cursor = conn.cursor()
    
    # Clear existing data to avoid duplicates if script is re-run
    try:
        logging.info("Clearing existing data from HealthRecord table...")
        cursor.execute("DELETE FROM dbo.HealthRecord")
        # Reset identity seed
        cursor.execute("DBCC CHECKIDENT ('dbo.HealthRecord', RESEED, 0)")
        conn.commit()
        logging.info("HealthRecord table cleared.")
    except pyodbc.ProgrammingError:
        logging.warning("HealthRecord table not found, skipping delete.")
        conn.rollback()


    logging.info("Preparing to load HealthRecord fact table...")
    
    records_to_insert = []
    for index, row in df.iterrows():
        try:
            person_key = (row['Sex'], row['AgeCategory'], row['RaceEthnicityCategory'])
            state_key = (row['State'],)
            checkup_key = (row['LastCheckupTime'], row['CheckupRecency'])
            
            # Handle derived columns
            activity_level = 'Active' if row['PhysicalActivities'] == 'Yes' else 'Inactive'
            physical_activity_key = (row['PhysicalActivities'], activity_level)

            lifestyle_key = (row['SmokerStatus'], row['ECigaretteUsage'], row['AlcoholDrinkers'], 'Good')

            chronic_key = (
                row['HadDiabetes'], row['HadArthritis'], row['HadCOPD'], 
                row['HadKidneyDisease'], row['HadDepressiveDisorder'], 
                'No', row['HadSkinCancer']
            )

            # Map keys to IDs
            person_id = maps['person'][person_key]
            state_id = maps['state'][state_key]
            checkup_id = maps['checkup'][checkup_key]
            pa_id = maps['physical_activity'][physical_activity_key]
            lifestyle_id = maps['lifestyle'][lifestyle_key]
            chronic_id = maps['chronic'][chronic_key]

            # HeartDiseaseFlag: 1 if they had a heart attack or angina, else 0
            heart_disease_flag = 1 if row['HadHeartAttack'] == 'Yes' or row['HadAngina'] == 'Yes' else 0

            record = (
                person_id, state_id, checkup_id, pa_id, chronic_id, lifestyle_id,
                heart_disease_flag,
                row.get('PhysicalHealthDays'),
                row.get('MentalHealthDays'),
                row.get('SleepHours'),
                row.get('HeightInMeters'),
                row.get('WeightInKilograms'),
                row.get('BMI'),
                2022  # RecordYear
            )
            records_to_insert.append(record)
        except KeyError as e:
            logging.warning(f"Skipping row {index} due to missing key in dimension map: {e}. Row data: {row.to_dict()}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing row {index} for HealthRecord: {e}. Row data: {row.to_dict()}")
            continue

    if not records_to_insert:
        logging.info("No records to insert into HealthRecord table.")
        return

    try:
        sql = """
        INSERT INTO dbo.HealthRecord (
            PersonID, StateID, CheckupTimeID, PhysicalActivityID, ChronicDiseaseID, LifestyleID,
            HeartDiseaseFlag, PhysicalHealthDays, MentalHealthDays, SleepHours,
            HeightInMeters, WeightInKilograms, BMI, RecordYear
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        cursor.executemany(sql, records_to_insert)
        conn.commit()
        logging.info(f"Successfully inserted {len(records_to_insert)} records into HealthRecord.")
    except Exception as e:
        logging.error(f"Failed to bulk insert into HealthRecord: {e}")
        conn.rollback()


# --- Main Execution ---
def main():
    """Main function to run the ETL process."""
    conn = get_db_connection()
    if conn is None:
        return

    logging.info(f"Reading data from {CSV_FILE_PATH}...")
    try:
        df = pd.read_csv(CSV_FILE_PATH, low_memory=False)
        # Basic data cleaning
        df = df.fillna({
            'PhysicalHealthDays': 0,
            'MentalHealthDays': 0,
            'SleepHours': df['SleepHours'].mean(),
            'HeightInMeters': df['HeightInMeters'].mean(),
            'WeightInKilograms': df['WeightInKilograms'].mean(),
            'BMI': df['BMI'].mean()
        })
        # Ensure 'Yes'/'No' columns are consistent
        for col in ['HadHeartAttack', 'HadAngina', 'HadStroke', 'HadAsthma', 'HadSkinCancer', 'HadCOPD', 
                    'HadDepressiveDisorder', 'HadKidneyDisease', 'HadArthritis', 'HadDiabetes', 'PhysicalActivities', 'AlcoholDrinkers']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: 'Yes' if x == 'Yes' else 'No')

    except FileNotFoundError:
        logging.error(f"Error: The file {CSV_FILE_PATH} was not found.")
        return
    except Exception as e:
        logging.error(f"An error occurred while reading the CSV file: {e}")
        return

    logging.info("Starting dimension table loading...")
    
    # DimPerson
    person_map = load_dimension(conn, df, 'DimPerson', 
                                {'Sex': 'Sex', 'AgeCategory': 'AgeCategory', 'RaceEthnicityCategory': 'RaceEthnicityCategory'},
                                ['Sex', 'AgeCategory', 'RaceEthnicityCategory'], 'PersonID')

    # DimState
    state_map = load_dimension(conn, df, 'DimState', {'State': 'StateName'}, ['StateName'], 'StateID')

    # DimCheckupTime
    df['CheckupRecency'] = df['LastCheckupTime'].apply(lambda x: 1 if 'year' in x else 5)
    checkup_map = load_dimension(conn, df, 'DimCheckupTime', {'LastCheckupTime': 'LastCheckupTime', 'CheckupRecency': 'CheckupRecency'}, ['LastCheckupTime'], 'CheckupTimeID')

    # DimPhysicalActivity
    df['ActivityLevel'] = df['PhysicalActivities'].apply(lambda x: 'Active' if x == 'Yes' else 'Inactive')
    pa_map = load_dimension(conn, df, 'DimPhysicalActivity', {'PhysicalActivities': 'PhysicalActivities', 'ActivityLevel': 'ActivityLevel'}, ['PhysicalActivities', 'ActivityLevel'], 'PhysicalActivityID')

    # DimLifestyle
    df['SleepQuality'] = 'Good' # Placeholder
    lifestyle_map = load_dimension(conn, df, 'DimLifestyle', 
                                   {'SmokerStatus': 'SmokerStatus', 'ECigaretteUsage': 'ECigaretteUsage', 'AlcoholDrinkers': 'AlcoholDrinkers', 'SleepQuality': 'SleepQuality'},
                                   ['SmokerStatus', 'ECigaretteUsage', 'AlcoholDrinkers', 'SleepQuality'], 'LifestyleID')

    # DimChronicDiseases
    df['HadCancer'] = 'No' # Add placeholder column
    chronic_map = load_dimension(conn, df, 'DimChronicDiseases',
                                 {'HadDiabetes': 'HadDiabetes', 'HadArthritis': 'HadArthritis', 'HadCOPD': 'HadCOPD', 
                                  'HadKidneyDisease': 'HadKidneyDisease', 'HadDepressiveDisorder': 'HadDepressiveDisorder', 
                                  'HadCancer': 'HadCancer', 'HadSkinCancer': 'HadSkinCancer'},
                                 ['HadDiabetes', 'HadArthritis', 'HadCOPD', 'HadKidneyDisease', 'HadDepressiveDisorder', 'HadCancer', 'HadSkinCancer'], 'ChronicDiseaseID')

    logging.info("Dimension loading complete.")

    # Load Fact Table
    maps = {
        'person': person_map,
        'state': state_map,
        'checkup': checkup_map,
        'physical_activity': pa_map,
        'lifestyle': lifestyle_map,
        'chronic': chronic_map
    }
    load_fact_table(conn, df, maps)

    conn.close()
    logging.info("ETL process finished.")

if __name__ == '__main__':
    main()
