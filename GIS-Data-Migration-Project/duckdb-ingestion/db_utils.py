def handle_existing_data(conn, table_to_check, job_name, truncate_commands=None):
    """
    Checks if a table has data and prompts the user to continue.
    Returns True if the script should proceed (either table is empty, user said yes, or error occurred).
    Returns False if the script should skip this specific part (user said no).
    """
    try:
        # Check if the table exists and has data
        result = conn.execute(f"SELECT COUNT(*) FROM {table_to_check}").fetchone()
        count = result[0] if result else 0
        
        if count > 0:
            print(f"Warning: Found {count} records in {table_to_check} table. Running this script may result in duplicate data.")
            user_input = input(f"Do you want to continue and truncate existing data for {job_name}? (yes/no): ")
            
            if user_input.strip().lower() == 'yes':
                if truncate_commands:
                    for cmd in truncate_commands:
                        try:
                            conn.execute(cmd)
                        except Exception as e:
                            print(f"Error executing truncate command: {e}")
                else:
                    print(f"Truncating data in table '{table_to_check}'...")
                    conn.execute(f"DELETE FROM {table_to_check};")
                return True
            else:
                print(f"Skipping {job_name} job.")
                return False
    except Exception:
        # If the table doesn't exist or another error occurs, assume it's safe to proceed
        pass
    
    return True

def string_to_bool(value):
    """Convert string '1'/'0', 'Yes'/'No' or empty to boolean. Returns False for empty, '0', or 'No', True for '1' or 'Yes'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        return v in ('1', 'yes', 'true')
    return bool(value) if value else False

def safe_int(value, default=0):
    """Safely convert string to int, returning default if conversion fails or value is empty."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    """Safely convert string to float, returning default if conversion fails or value is empty."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
