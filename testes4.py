from random import randint
import random
import string
from credentials import *
import oracledb
import time
from datetime import datetime

# Variables and isntances

letters = string.ascii_uppercase

un = USERNAME_ONP
pw = PASSWORD_ONP
cs = URL_ONP

inserts=0
updates=0

# oradb connection
connection = oracledb.connect(user=un, password=pw, dsn=cs)
cursor = connection.cursor()

cursor.execute("SELECT COUNT(1) FROM MASERA.BIGTABLE")
totalRecordsCursor = cursor.fetchone()
for row in totalRecordsCursor:
    totalRecords = row
    print('Total records before execution:',totalRecords)

queryRange = int(totalRecords + (totalRecords /20))

totalQueries = int(totalRecords/100)
print('Upserts to be executed: ', totalQueries)

maxQueryInterval = int(86400 / totalQueries)
minQueryInterval = int(maxQueryInterval / 4)
print('Minumum time between queries: ',minQueryInterval)
print('Maximum time between queries: ',maxQueryInterval)

startTime = datetime.now()
print('Starting processes of upserts:',startTime)


# Loop for insert and updates generation
for i in range(1,int(totalQueries)):
    cursor.execute("COMMIT")
    waittiming = int(randint(minQueryInterval,maxQueryInterval))
    #time.sleep(waittiming)
    v1   = ( ''.join(random.choice(letters) for i in range(20)) )
    v2	 = ( ''.join(random.choice(letters) for i in range(20)) )    
    GENID = int(randint(1,queryRange))
    cursor.execute("SELECT COUNT(1) FROM MASERA.BIGTABLE WHERE ID= :TEMPID",TEMPID=GENID)
    control = cursor.fetchone()
    for row in control:
        control = row
    if control == 1:
        updates = updates +1
        cursor.execute("UPDATE MASERA.BIGTABLE SET COL1=:sv1,COL2=:sv2,DATA_REF=CURRENT_DATE WHERE ID=:TEMPID",sv1=v1,sv2=v2,TEMPID=GENID)
    else:
        inserts = inserts +1
        cursor.execute("INSERT INTO MASERA.BIGTABLE VALUES (MASERA.BIGTABLE_SEQ.NEXTVAL,:sv1,:sv2,CURRENT_DATE)", sv1=v1,sv2=v2)

# Total results
finishTime = datetime.now()
print('Hour of conclusion:',finishTime)
print('Total Inserts: ',inserts)
print('Total Updates:',updates)