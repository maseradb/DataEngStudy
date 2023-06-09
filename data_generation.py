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
print('Connected to the database')
cursor = connection.cursor()
print('Starting loop')
for i in range (1, 1000000):
	print('Running the insert number:',i)
	v1   = (''.join(random.choice(letters) for i in range(20)))
	v2	 = (''.join(random.choice(letters) for i in range(20)))
	v3	 = (''.join(random.choice(letters) for i in range(20)))
	cursor.execute("INSERT INTO MASERA.DDL_TESTS2 VALUES (MASERA.DDL_TESTS2_SEQ.NEXTVAL,:sv1,:sv2,:sv3,SYSDATE)", sv1=v1,sv2=v2,sv3=v3)
	cursor.execute("COMMIT")
	print('Insert commited!')
	nextwait=int(random.randint(10,60))
	print('Waiting', nextwait ,'seconds for the next insert')
	time.sleep(nextwait)	
connection.commit()
cursor.close()
connection.close()