from fastapi import FastAPI
import pyodbc
from typing import List, Dict

# Database setup
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:umng-experiment.database.windows.net,1433;"
    "Database=datos-experimentos;"
    "Uid=JuanChurio;"
    "Pwd=UmngExperimento4$;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)
connection = pyodbc.connect(DATABASE_CONNECTION_STRING)

# FastAPI app instance
app = FastAPI()

def insertRows(data: dict):
    cursor = connection.cursor()
    try:
        sql = '''
            INSERT INTO DatosExperimento (CONDITION_A, CONDITION_B, GRAPH, timeTaken, Error, controlCondition, timePer)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(sql, (
            data['CONDITION_A'],
            data['CONDITION_B'],
            data['GRAPH'],
            data['timeTaken'],
            data['Error'],
            data['controlCondition'],
            data['timePer']
        ))
        cursor.commit()
    finally:
        cursor.close()
    return {"status": "success", "message": "Row inserted successfully"}

# Function to execute a SELECT query
def get_experiment_data() -> List[Dict]:
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM DatosExperimento")
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]  # Extract column names
    cursor.close()
    return [dict(zip(columns, row)) for row in rows]

@app.post("/insertRows/")
async def insert_experiment_data(data: dict):
    return insertRows(data)

@app.get("/experiment_data/")
async def get_experiment_data_endpoint():
    return get_experiment_data()

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}