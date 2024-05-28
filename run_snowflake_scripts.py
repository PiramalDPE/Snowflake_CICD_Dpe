import os
import snowflake.connector

# Retrieve environment variables
account = os.getenv('SNOWFLAKE_ACCOUNT')
user = os.getenv('SNOWFLAKE_USER')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
role = os.getenv('SNOWFLAKE_ROLE')
private_key_path = 'private_key.pem'

# Establish the Snowflake connection
ctx = snowflake.connector.connect(
    user=user,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role,
    private_key=open(private_key_path).read()
)

cs = ctx.cursor()

# List of SQL files to be executed
sql_files = [
    'scripts/create_tables.sql',
    'scripts/load_data.sql',
    'scripts/run_transformations.sql'
]

try:
    for sql_file in sql_files:
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
            cs.execute(sql_commands)
finally:
    cs.close()
    ctx.close()
