import oracledb
import random
import string
from credentials import *
import time

letters = string.ascii_uppercase

un = USERNAME_ONP
pw = PASSWORD_ONP
cs = URL_ONP

connection = oracledb.connect(user=un, password=pw, dsn=cs)
cursor = connection.cursor()
for i in range (1, 1000000):
	if (i % 5000==0): print(i)
	time.sleep(int(random.randint(10,60)))
	cursor.execute("COMMIT")
	v1   = (''.join(random.choice(letters) for i in range(20)))
	v2	 = (''.join(random.choice(letters) for i in range(20)))
	cursor.execute("INSERT INTO MASERA.STREAMTABLE VALUES (MASERA.STREAMTABLE_SEQ.NEXTVAL,:sv1,:sv2,SYSDATE-DBMS_RANDOM.value(0,1092))", sv1=v1,sv2=v2)
connection.commit()
cursor.close()
connection.close()