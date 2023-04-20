import oracledb
import random
import string
from credentials import *

letters = string.ascii_uppercase

un = USERNAME_OCI
pw = PASSWORD_OCI
cs = URL_OCI

connection = oracledb.connect(user=un, password=pw, dsn=cs)
cursor = connection.cursor()
for i in range (1, 1000000):
	if (i % 5000==0): print(i)
	cursor.execute("COMMIT")
	v1   = (''.join(random.choice(letters) for i in range(20)))
	v2	 = (''.join(random.choice(letters) for i in range(20)))
	cursor.execute("INSERT INTO ALESSANDRO.BIGTABLE VALUES (ALESSANDRO.BIGTABLE_SEQ.NEXTVAL,:sv1,:sv2,SYSDATE-DBMS_RANDOM.value(0,1092))", sv1=v1,sv2=v2)
connection.commit()
cursor.close()
connection.close()