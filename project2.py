import mysql.connector
from mysql.connector import Error


try:
    connection = mysql.connector.connect(host='localhost',
                                         database='project2',
                                         user='root',
                                         password='password')


    if connection.is_connected():

        #creates a new table for customer data
        query = """CREATE TABLE `project2`.`customer` (
                `custID` INT NOT NULL,
                `custName` VARCHAR(45) NULL,
                `zip` VARCHAR(45) NULL,
                `city` VARCHAR(45) NULL,
                `state` VARCHAR(45) NULL,
               PRIMARY KEY (`custID`)); """
        cursor = connection.cursor()
        cursor.execute(query)
        print("Successfully created the customer table")

        #inserts the first customer
        query = """INSERT INTO CUSTOMER VALUES ('1','STEPHEN WALTHER','06410','Cheshire','CT')"""
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Customer 1 inserted successfully into the table")

        #inserts the second customer
        query = """INSERT INTO CUSTOMER VALUES ('2','JAMES GOODWILL','55014','Circle Pines','MN')"""
        cursor.execute(query)
        connection.commit()
        print("Customer 2 inserted successfully into the table")

        #inserts the third customer
        query = """INSERT INTO CUSTOMER VALUES ('3','CALVIN HARRIS','77566','Lake Jackson','TX')"""
        cursor.execute(query)
        connection.commit()
        print("Customer 3 inserted successfully into the table")

        #inserts the fourth customer
        query = """INSERT INTO CUSTOMER VALUES ('4','MARTIN GARRIX','39759','Starkville','MS')"""
        cursor.execute(query)
        connection.commit()
        print("Customer 4 inserted successfully into the table")

        #inserts the fifth customer
        query = """INSERT INTO CUSTOMER VALUES ('5','PAMELA REIF','48047','New Baltimore','MI')"""
        cursor.execute(query)
        connection.commit()
        print("Customer 5 inserted successfully into the table")

        #grabs title where publisher name starts with a T
        query = """
                select title
                from publishers, titles
                where (publishers.pname like 'T%' 
                and publishers.pubID = titles.pubID)   
                """
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)
        #inserts a new titleauthor value to the table
        query = """
                INSERT IGNORE INTO TITLEAUTHORS VALUES (1004,103,1); 
                """
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

        #updates prices of titles where date is before 2004
        query = """
                update titles
                set price = price * 0.7
                where ( pubDate <= '2004-12-31' ); 
                """
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

        #updates prices of titles where date is after 2004
        query = """
                update titles
                set price = price * 0.85
                where ( pubDate > '2004-12-31' ) 
                """
        cursor.execute(query)
        connection.commit()

        #Will show the completed query
        query = """
                select * from titles
                """
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)

        query = """
                select aName
                from titleauthors, authors 
                where authors.auID = titleauthors.auID and aName != 'HERBERT SCHILD' and titleID = 
                (select titleID
                from titleauthors
                where auID = 
                (select auID
                from authors
                where aName = 'HERBERT SCHILD'));  
                """
        cursor = connection.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)

except Error as e:
    print("Error while connection to MySQL",e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
