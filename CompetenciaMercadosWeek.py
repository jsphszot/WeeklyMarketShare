"""
Read SQL scripts, apply values to formatted string, 
run query and write df results to specifically named csv (for excel reading)
"""

from google.cloud import bigquery
import google.auth

# Explicitly create a credentials object.
# see https://cloud.google.com/docs/authentication/
credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

# Make client.
bqclient = bigquery.Client(credentials=credentials, project=your_project_id,)

def runBQSQLscript(filename, backweeks=1):
    """
    Runs sql in filename, returns DataFrame

    filename: name (path) to sql to be run
    backweess: how many weeks back to consider as "last week", default is 1
    """
    fd = open(filename, 'r')
    sqlFile = fd.read().format(back_weeks=backweeks)
    dfWeekAWB = bqclient.query(sqlFile).result().to_dataframe()
    return dfWeekAWB


ScriptWeekAWB=runBQSQLscript("CompMSweek-AWB.sql", 2)
ScriptWeekFeeder=runBQSQLscript("CompMSweek-Feeder.sql", 2)
ScriptWeekGTWY=runBQSQLscript("CompMSweek-GTWY.sql", 2)

ScriptWeekAWB.to_csv("inputs for excel/dfWeekAWB.csv", index=False)
ScriptWeekFeeder.to_csv("inputs for excel/dfWeekFeeder.csv", index=False)
ScriptWeekGTWY.to_csv("inputs for excel/dfWeekGTWY.csv", index=False)
