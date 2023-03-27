#!/usr/bin/env python3
# note user or service account need 
#   IAM Project Role: Service Usage Consumer 
#   Spanner Database Permisisons

from google.cloud import spanner
import datetime
from numpy import random
from faker import Faker
import os
import time
fake = Faker()
Faker.seed(random.randint(1000))
project=os.environ.get("GOOGLE_CLOUD_PROJECT")
#project="INSERT-PROJECT-ID"
    
def readtable(project_id,instance_id,database_id):
    print("SINGLE READ: Database: " + instance_id + " Table: " + database_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)

def readtableboundedstale(project_id,instance_id,database_id,staleseconds=15):
    print("SINGLE READ BOUNDED/MAX STALE: Reading Database: " + instance_id + " Table: " + database_id + "Stale: " + str(staleseconds))
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    staleness = datetime.timedelta(seconds=staleseconds)
    with database.snapshot(max_staleness=staleness) as snapshot:
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)

def readtableexactstale(project_id,instance_id,database_id,staleseconds=15):
    print("SINGLE READ EXACT STALE: Reading Database: " + instance_id + " Table: " + database_id + "Stale: " + str(staleseconds))
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    staleness = datetime.timedelta(seconds=staleseconds)
    with database.snapshot(exact_staleness=staleness) as snapshot:
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)

def readtablenotransactional(project_id,instance_id,database_id):
    print("SINGLE NON-TRANSACTIONAL READ: Reading Database: " + instance_id + " Table: " + database_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.snapshot(multi_use=False) as snapshot:
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)
        try:
            results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        except Exception as e:
            print("ERROR: " + str(e))
        else:
            for row in results:
                print(row)

def readtabletransactional(project_id,instance_id,database_id):
    print("TRANSACTIONAL READ (Mulit-use True): Reading Database: " + instance_id + " Table: " + database_id)
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    with database.snapshot(multi_use=True) as snapshot:
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)
        input("Add a row and then press Enter to continue...")
        results = snapshot.execute_sql("Select firstname,lastname,employeeid from names")
        for row in results:
            print(row)
        
def insertnameintonames(transaction):
    employeeid = random.randint(100000)
    firstname = fake.first_name()
    lastname = fake.last_name()
    newrow = str(employeeid) + ",'" + firstname + "','" + lastname + "'"
    try:
        insert = transaction.execute_update(
            "INSERT INTO names (employeeid, firstname, lastname) Values"
            "(" + newrow + ")"
            )
    except Exception as e:
        print("ERROR: " + str(e))
    else:
        print("Row " + newrow + " Inserted")

def writetotable(project_id,instance_id,database_id):
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    print("Inserting Row")
    database.run_in_transaction(insertnameintonames)

def writetotableloop(project_id,instance_id,database_id,numrows=100,delay=1):
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    for rows in range (1,numrows):
        database.run_in_transaction(insertnameintonames)
        print("Inserted Row " + str(rows) + " of " + str(numrows))
        time.sleep(delay)

if __name__ == "__main__":
    print("Using project " + project)
    readtable(project,"regionalspanner","demo",)
    readtableboundedstale(project,"regionalspanner","demo")
    readtableexactstale(project,"regionalspanner","demo")
    readtablenotransactional(project,"regionalspanner","demo")
    readtabletransactional(project,"regionalspanner","demo")
    writetotable(project,"regionalspanner","demo")
    readtable(project,"regionalspanner","demo")
    #writetotableloop(project,"regionalspanner","demo",100,5)