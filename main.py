from fastapi import FastAPI, HTTPException
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

# FastAPI app instance
app = FastAPI()

def get_connection():
    return pyodbc.connect(DATABASE_CONNECTION_STRING)

def insertRows(data: dict):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        sql = '''
            INSERT INTO segundaIteracion (ID,gender,age,visionImpediment,CONDITION_A, CONDITION_B, GRAPH, timeTaken, Error, controlCondition, timePer)
            VALUES (?,?,?,?,?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(sql, (
            data['ID'],
            data['gender'],
            data['age'],
            data['visionImpediment'],
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
        connection.close()
    return {"status": "success", "message": "Row inserted successfully"}

def insertUser(data: dict):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        sql = '''
            INSERT INTO caracterizacion (ID, gender, age, visionImpediment)
            VALUES (?, ?, ?, ?)
        '''
        cursor.execute(sql, (
            data['ID'],
            data['gender'],
            data['age'],
            data['visionImpediment']
        ))
        cursor.commit()
    finally:
        cursor.close()  # Close the cursor
        connection.close()  # Close the connection
    return {"status": "success", "message": "Row inserted successfully"}


def getLatestUser():
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(id) FROM caracterizacion")
        latest_id = cursor.fetchone()[0]
        return latest_id
    finally:
        cursor.close()
        connection.close()

# Function to execute a SELECT query
def get_experiment_data() -> List[Dict]:
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM DatosExperimento")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]  # Extract column names
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        connection.close()

@app.post("/insertRows/")
async def insert_experiment_data(data: dict):
    return insertRows(data)

@app.post("/insertUser/")
async def insert_user_data(data: dict):
    return insertUser(data)

@app.get("/getLatestUser/")
async def getLatest():
    return getLatestUser()

@app.get("/experiment_data/")
async def get_experiment_data_endpoint():
    return get_experiment_data()

@app.get("/")
async def read_root():
    return {"message": "Hello, World!"}
