#!/usr/bin/env python3
# note user or service account need 
#   IAM Project Role: Service Usage Consumer 
#   Spanner Instance Admin Permissions

from google.cloud import spanner
import os
OPERATION_TIMEOUT_SECONDS = 240
project=os.environ.get("GOOGLE_CLOUD_PROJECT")
#project="INSERT-PROJECT-ID"

def createdatabase(project_id,instance_id,database_id):
    print("Creating Database: " + database_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    try: 
        operation = database.create()
        operation.result(OPERATION_TIMEOUT_SECONDS)
    except Exception as e:
        print("ERROR: " + str(e))
    else:
        print("Database Created: " + database_id)

def createtable(project_id,instance_id,database_id,table_id):
    print("Creating Table " + table_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    tablecommand = [ "Create Table " + table_id + "("
            "employeeid  INT64 NOT NULL,"
            "firstname STRING(40),"
            "lastname STRING(40)"
            ") PRIMARY KEY (employeeid)"
            ]
    try: 
        operation = database.update_ddl(tablecommand)
        operation.result(OPERATION_TIMEOUT_SECONDS)
    except Exception as e:
        print("ERROR: " + str(e))
    else:
        print("Table " + table_id + " Created")

def droptable(project_id,instance_id,database_id,table_id):
    print("Dropping " + table_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    tablecommand = [ "Drop Table " + table_id 
            ]
    try: 
        operation = database.update_ddl(tablecommand)
        operation.result(OPERATION_TIMEOUT_SECONDS)
    except Exception as e:
        print("ERROR: " + str(e))
    else:
        print("Table " + table_id + " Dropped")

if __name__ == "__main__":
    print("Using project " + project)
    instance="regionalspanner"
    database="demo"
    createdatabase(project,instance,database)
    table="names"
    createtable(project,instance,database,table)
    table="lotsofnames"
    createtable(project,instance,database,table)
    #droptable(project,instance,database,table)
    