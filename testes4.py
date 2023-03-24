from random import randint
import random
import string
from credentials import *
import oracledb
import time

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

# Loop for insert and updates generation
for i in range(1,10000):
    cursor.execute("COMMIT")
    waittiming = int(randint(3,8))
    time.sleep(waittiming)
    v1   = ( ''.join(random.choice(letters) for i in range(20)) )
    v2	 = ( ''.join(random.choice(letters) for i in range(20)) )    
    GENID = int(randint(1,1200000))
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
print('Total Inserts:')
print(inserts)
print('Total Updates:')
print(updates)