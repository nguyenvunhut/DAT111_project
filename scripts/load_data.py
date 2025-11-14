import csv
import pyodbc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
CSV_FILE_PATH = r'D:\Learning\FPT_polytechnic\Sem4\DAT111\DAT111_project\data\processed\final_data.csv'
SERVER_NAME = r'NHUTVU'
DATABASE_NAME = 'HeartDiseaseDB'
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

def load_dimension(conn, data, table_name, column_map, unique_cols, id_column_name):
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
    data_to_insert = []
    seen_keys = set()
    for row in data:
        key_tuple = tuple(row.get(col) for col in column_map.keys())
        if key_tuple not in seen_keys:
            seen_keys.add(key_tuple)
            # Create a dictionary with only the required columns
            insert_row = {col: row.get(col) for col in column_map.keys()}
            data_to_insert.append(insert_row)
    
    id_map = existing_data.copy()
    new_rows = 0

    for row in data_to_insert:
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
                logging.error(f"Error inserting into {table_name}: {e}. Problematic row: {row}")
                conn.rollback()


    if new_rows > 0:
        conn.commit()
        logging.info(f"Inserted {new_rows} new records into {table_name}.")
    else:
        logging.info(f"No new records to insert into {table_name}.")

    return id_map

def load_fact_table(conn, data, maps):
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
    for index, row in enumerate(data):
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
            logging.warning(f"Skipping row {index} due to missing key in dimension map: {e}. Row data: {row}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing row {index} for HealthRecord: {e}. Row data: {row}")
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


# --- Helper Functions ---
def calculate_means(data, columns):
    """Calculates the mean for specified columns in a list of dictionaries."""
    sums = {col: 0 for col in columns}
    counts = {col: 0 for col in columns}
    for row in data:
        for col in columns:
            if row.get(col) and row[col] not in ['NA', '', 'None']:
                try:
                    sums[col] += float(row[col])
                    counts[col] += 1
                except (ValueError, TypeError):
                    continue  # Ignore non-numeric values
    return {col: sums[col] / counts[col] if counts[col] > 0 else 0 for col in columns}


# --- Main Execution ---
def main():
    """Main function to run the ETL process."""
    conn = get_db_connection()
    if conn is None:
        return

    logging.info(f"Reading data from {CSV_FILE_PATH}...")
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            data = list(reader)

        # --- Basic data cleaning ---
        numeric_cols_for_mean = ['SleepHours', 'HeightInMeters', 'WeightInKilograms', 'BMI']
        means = calculate_means(data, numeric_cols_for_mean)

        # Define columns to be processed
        yes_no_cols = [
            'HadHeartAttack', 'HadAngina', 'HadStroke', 'HadAsthma', 'HadSkinCancer', 
            'HadCOPD', 'HadDepressiveDisorder', 'HadKidneyDisease', 'HadArthritis', 
            'HadDiabetes', 'PhysicalActivities', 'AlcoholDrinkers'
        ]
        
        numeric_cols_to_fill = ['PhysicalHealthDays', 'MentalHealthDays'] + numeric_cols_for_mean

        for row in data:
            # Fill missing numeric values
            for col in numeric_cols_to_fill:
                if not row.get(col) or row[col] in ['NA', '', 'None']:
                    row[col] = means.get(col, 0) # Use mean if available, else 0
            
            # Convert numeric columns to float
            for col in numeric_cols_to_fill:
                try:
                    row[col] = float(row[col])
                except (ValueError, TypeError):
                    row[col] = 0.0 # Default to 0.0 if conversion fails

            # Standardize 'Yes'/'No' columns
            for col in yes_no_cols:
                if col in row:
                    row[col] = 'Yes' if row[col] == 'Yes' else 'No'
            
            # Add derived/placeholder columns
            row['CheckupRecency'] = 1 if 'year' in row.get('LastCheckupTime', '') else 5
            row['ActivityLevel'] = 'Active' if row.get('PhysicalActivities') == 'Yes' else 'Inactive'
            row['SleepQuality'] = 'Good'  # Placeholder
            row['HadCancer'] = 'No'  # Placeholder


    except FileNotFoundError:
        logging.error(f"Error: The file {CSV_FILE_PATH} was not found.")
        return
    except Exception as e:
        logging.error(f"An error occurred while reading or cleaning the CSV file: {e}")
        return

    logging.info("Starting dimension table loading...")
    
    # DimPerson
    person_map = load_dimension(conn, data, 'DimPerson', 
                                {'Sex': 'Sex', 'AgeCategory': 'AgeCategory', 'RaceEthnicityCategory': 'RaceEthnicityCategory'},
                                ['Sex', 'AgeCategory', 'RaceEthnicityCategory'], 'PersonID')

    # DimState
    state_map = load_dimension(conn, data, 'DimState', {'State': 'StateName'}, ['StateName'], 'StateID')

    # DimCheckupTime
    checkup_map = load_dimension(conn, data, 'DimCheckupTime', {'LastCheckupTime': 'LastCheckupTime', 'CheckupRecency': 'CheckupRecency'}, ['LastCheckupTime'], 'CheckupTimeID')

    # DimPhysicalActivity
    pa_map = load_dimension(conn, data, 'DimPhysicalActivity', {'PhysicalActivities': 'PhysicalActivities', 'ActivityLevel': 'ActivityLevel'}, ['PhysicalActivities', 'ActivityLevel'], 'PhysicalActivityID')

    # DimLifestyle
    lifestyle_map = load_dimension(conn, data, 'DimLifestyle', 
                                   {'SmokerStatus': 'SmokerStatus', 'ECigaretteUsage': 'ECigaretteUsage', 'AlcoholDrinkers': 'AlcoholDrinkers', 'SleepQuality': 'SleepQuality'},
                                   ['SmokerStatus', 'ECigaretteUsage', 'AlcoholDrinkers', 'SleepQuality'], 'LifestyleID')

    # DimChronicDiseases
    chronic_map = load_dimension(conn, data, 'DimChronicDiseases',
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
    load_fact_table(conn, data, maps)

    conn.close()
    logging.info("ETL process finished.")

if __name__ == '__main__':
    main()
