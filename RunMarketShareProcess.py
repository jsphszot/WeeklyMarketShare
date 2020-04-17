"""

Read SQL scripts, run query and write df results to specifically named csv (for excel input)

Proceso de lectura, edición, y carga a GBQ de datos Week Mercados de Microstrategy guardado en carpeta "BajadaMSTR"
Luego corre scipts sql para agrupar y reprocesar data, la cual escribe a csv en carpeta "inputs for excel"

"""

SharedFolderLatam=r"\\3kusmiafs02.amn.lan.com\CAPACITY\Planificacion y Analisis Comercial\Gestion de Resultados\1. Gestion de Resultados Semanal\2. Resultados\CompMercadosWeek"
ProcessFolder=r"C:\SchTasks\CompMercadosWeek"
# ProcessFolder=r"G:\My Drive\schTasks\CompMercadosWeek"

# jsongcpauthfile=r"C:\SchTasks\credentials.json"
jsongcpauthfile=ProcessFolder+r"\credentials.json" # js

backweeks=input("Please provide backweeks:")

# libraries, path and user defined functions
# region --------------------------------------------------------------------- 

import cmd
import pandas as pd
import re
from datetime import datetime
from google.cloud import bigquery
import google.auth

def CleanColNames(df):
    return [re.sub("\s+|^\d+|\(|\)|\&", "", x) for x in df.columns]

def weeks_for_year(year):
    last_week = datetime(year, 12, 28)
    return last_week.isocalendar()[1]

def delta_weeks(year):
    """
    gets "Distance in Weeks" of a past year from this year\n
    \tSubtract this value from a Week Number for the past year to get the relative week.
    \tExample: If this year is 2020, than 2018 is 52+52 weeks past from 2020.
    \t         Week 50 of 2018 is then week 50-(52+52) = -54 relative to this Year.
    """
    currentyear = datetime.today().isocalendar()[0]
    inyear = datetime(year, 12, 28).isocalendar()[1]
    yearrange=list(range(year, currentyear))
    return sum([weeks_for_year(x) for x in yearrange])

def runBQSQLscript(filename, backweeks=1):
    """
    Runs sql in filename, returns DataFrame

    filename: name (path) to sql to be run
    backweess: how many weeks back to consider as "last week", default is 1
    """
    fd = open(filename, 'r')
    sqlFile = fd.read().format(back_weeks=backweeks)
    dfWeekAWB = client.query(sqlFile).result().to_dataframe()
    return dfWeekAWB


# endregion 
# -----------------------------------------------------------------------------

client = bigquery.Client.from_service_account_json(jsongcpauthfile)

# read and edit MSTR xlsx file -----------------------------------------------
# region

WkMcdosMSTR=pd.read_excel(SharedFolderLatam+r"\BajadaMSTR\Week Mercados.xlsx", skiprows=2)
WkMcdosMSTR.columns=CleanColNames(WkMcdosMSTR)
WkMcdosMSTR['RelWeek']=WkMcdosMSTR['Semana']-[delta_weeks(x) for x in WkMcdosMSTR['Año']]

set(WkMcdosMSTR['Owner'])

WkMcdosMSTR['Owner'] = [x if x != "QT " else "AV " for x in WkMcdosMSTR['Owner']]

newcolnames=[
    'Year',
    'Semana',
    'RegionOrigenAWB',
    'RegionOrigenSegmento',
    'ZonaOrigenAWB',
    'PaisOrigenAWB',
    'PaisOrigenSegmento',
    'PaisDestinoAWB',
    'PostaDestinoAWB',
    'TipoVuelo',
    'Owner',
    'Tons',
    'RelWeek',
    ]

WkMcdosMSTR.columns=newcolnames

# TODO add updated data verification (send to slack?)
# maxyear=WkMcdosMSTR.Year.max()
# datetime.today().isocalendar()[1]-1 == WkMcdosMSTR.query(f"Year == {maxyear}").Semana.max()
# set(WkMcdosMSTR.query(f"Year == {maxyear} and PostaDestinoAWB in ['GRU', 'VCP']").Semana)

# endregion
# ----------------------------------------------------------------------------


# Upload to GBQ --------------------------------------------------------------
# region

# Upload to GBQ
schema = [
    bigquery.SchemaField('Year', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('Semana', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('RegionOrigenAWB', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('RegionOrigenSegmento', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('ZonaOrigenAWB', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PaisOrigenAWB', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PaisOrigenSegmento', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PaisDestinoAWB', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PostaDestinoAWB', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('TipoVuelo', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Owner', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('Tons', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('RelWeek', 'INTEGER', mode='NULLABLE'),
    ]

dataset_name = 'ReporteWeek'
table_name = 'McdoBASE'

dataset_ref = client.dataset(dataset_name)
table_ref = dataset_ref.table(table_name)
table = bigquery.Table(table_ref, schema=schema)
## create table if doesn't exist, delete
client.create_table(table, exists_ok=True)
#client.delete_table(table_ref)

job_config = bigquery.LoadJobConfig()
job_config.create_disposition = 'CREATE_IF_NEEDED'
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
job_config.schema = schema
client.load_table_from_dataframe(dataframe=WkMcdosMSTR,destination=table_ref,job_config=job_config).result()

# endregion
# ----------------------------------------------------------------------------


# Run queries and write Dashboard csv inputs ---------------------------------
# region

# backweeks=1 # default for function is 1 anyways

# ScriptWeeks sin Tipo Vuelo (durante covid se decidió agrupar vistas)
ScriptWeekAWBstv=runBQSQLscript(ProcessFolder+r"\CompMSweek-covid-AWB.sql", backweeks)
ScriptWeekGTWYstv=runBQSQLscript(ProcessFolder+r"\CompMSweek-covid-GTWY.sql", backweeks)

# scripts base (agrupaciones originales de presentaciones de mercado)
ScriptWeekAWB=runBQSQLscript(ProcessFolder+r"\CompMSweek-AWB.sql", backweeks)
ScriptWeekFeeder=runBQSQLscript(ProcessFolder+r"\CompMSweek-Feeder.sql", backweeks)
ScriptWeekGTWY=runBQSQLscript(ProcessFolder+r"\CompMSweek-GTWY.sql", backweeks)

ScriptWeekAWB.append(ScriptWeekAWBstv).drop_duplicates().to_csv(SharedFolderLatam+r"\inputs for excel\dfWeekAWB.csv", index=False)
ScriptWeekFeeder.to_csv(SharedFolderLatam+r"\inputs for excel\dfWeekFeeder.csv", index=False)
ScriptWeekGTWY.append(ScriptWeekGTWYstv).drop_duplicates().to_csv(SharedFolderLatam+r"\inputs for excel\dfWeekGTWY.csv", index=False)


# endregion
# ----------------------------------------------------------------------------


print("Mercados corrió con éxito")

# https://datatofish.com/batch-python-script/
# import tkinter as tk

# root=tk.Tk()
# canvas1 = tk.Canvas(root, width = 300, height = 300)
# canvas1.pack()
# button1 = tk.Button(root, text='Mercados corrió con éxito', command=root.destroy)

# canvas1.create_window(150, 150, window=button1)
# root.mainloop()
