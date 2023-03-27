#!/usr/bin/env python3
# note user or service account need 
#   IAM Project Role: Service Usage Consumer 
#   Spanner Database Permisisons

from google.cloud import spanner
import datetime as datetime
from numpy import random
from faker import Faker
import os
import time
fake = Faker()
Faker.seed(random.randint(1000))
project=os.environ.get("GOOGLE_CLOUD_PROJECT")
#project="INSERT-PROJECT-ID"

def getdatetimestamp():
    currentdate = datetime.datetime.now()
    returnstring = currentdate.strftime("%m%d%H%m%S%f")
    return returnstring

def newrow():
    employeeid = int(getdatetimestamp())
    firstname = fake.first_name()
    lastname = fake.last_name()
    newrow = "(" + str(employeeid) + ",'" + firstname + "','" + lastname + "')"
    print("Row: " + newrow)
    return newrow

def writerows(transaction):
    numrows=100
    rows=newrow()
    for newrows in range (1,numrows):
        time.sleep(.002)
        rows = rows + "," + newrow()
    rowcount = transaction.execute_update("INSERT INTO lotsofnames (employeeid, firstname, lastname) Values " + rows)
    print("{} row(s) inserted".format(rowcount))    

    
def writetotable(project_id,instance_id,database_id,table_id,count=100):
    spanner_client = spanner.Client(project=project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    batchcount = int(count/100)
    for batch in range(0,batchcount):
        database.run_in_transaction(writerows)

if __name__ == "__main__":
    print("Using project " + project)
    instance="regionalspanner"
    database="demo"
    table="allnames"
    numrows=10000
    writetotable(project,"regionalspanner","demo","allnames",numrows)