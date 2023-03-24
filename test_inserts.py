sqls = [
    [1,'INSERT INTO SPARK.SPARKUSERS (name,surname,phone) VALUES ("Rafael","Daniel","018947347852");'],
    [2,'INSERT INTO SPARK.SPARKUSERS (name,surname,phone) VALUES ("Ifeoma","Tyler","073985238489");'],
    [3,'INSERT INTO SPARK.SPARKUSERS (name,surname,phone) VALUES ("Jade","Schneider","074909189852");']
    ]



for sql in sqls:
    print(sql[1])
