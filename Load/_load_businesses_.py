import pandas as pd
import mysql.connector
from mysql.connector import errorcode



# the parameters to connect mysql
USER = 'root'
PASSWORD = 'Umtruelover1005'
HOST = '127.0.0.1'
DATABASE = 'yelp'

# delete tables if exit
delete_queries = ["drop table if exists businesses"]

# table structures
TABLES = {}
TABLES['businesses'] = (
    "   CREATE TABLE `businesses` ("
    "  `restaurant_name` VARCHAR(100) NOT NULL,"
    "  `score` FLOAT,"
    "  `review_num` INT,"
    "  `city` VARCHAR(50),"
    "  `id` VARCHAR(100) NOT NULl,"
    "  `latitude` FLOAT,"
    "  `longitude` FLOAT,"
    "  `categories` VARCHAR(100),"
    "  `state_name` VARCHAR(20) NOT NULL, "
    "  `state_code` VARCHAR(10) NOT NULL, "
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")


add_business = ("INSERT INTO businesses "
               "(restaurant_name, score, review_num, city, id, latitude, longitude, categories, state_name, state_code) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

if __name__ == '__main__':

    business_df = pd.read_csv('./crawl data/US_restaurant_csv.csv', header = 0, index_col=0)
    business_df['categories'] = business_df['categories'].str.strip()
    print(business_df.columns)

    try:
        cnx = mysql.connector.connect(user=USER, password=PASSWORD,
                                      host=HOST,
                                      database=DATABASE)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        print("connect mysql successfully!")
        cursor = cnx.cursor()

        # delete tables
        for delete_query in delete_queries:
            cursor.execute(delete_query)
        # create tables
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

        for index, row in business_df.iterrows():
            data_business = (row['Restaurant name'], row['rating score'], row['review count'], row['city'],
                             row['business id'], row['latitude'], row['longitude'], row['categories'], row['State'], row['State Code'])

            try:
                cursor.execute(add_business, data_business)
            except:
                print(data_business)




        # Make sure data is committed to the database
        cnx.commit()

        cursor.close()
        cnx.close()

