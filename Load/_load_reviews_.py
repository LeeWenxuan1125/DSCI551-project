import pandas as pd
import mysql.connector
from mysql.connector import errorcode
# the parameters to connect mysql
USER = 'root'
PASSWORD = 'Umtruelover1005'
HOST = '127.0.0.1'
DATABASE = 'yelp'

# delete tables if exit
delete_queries = ["drop table if exists reviews"]

# table structures
TABLES = {}
TABLES['reviews'] = (
    "   CREATE TABLE `reviews` ("
    "  `review_id` VARCHAR(100) NOT NULL,"
    "  `user_id` VARCHAR(100) NOT NULL,"
    "  `business_id` VARCHAR(100) NOT NULL,"
    "  `rating` FLOAT,"
    "  `date` DATETIME, "
    "  `text` TEXT, "
    "  PRIMARY KEY (`review_id`)"
    ") ENGINE=InnoDB")

add_reviews = ("INSERT INTO reviews "
               "(review_id, user_id, business_id, rating, date, text) "
               "VALUES (%s, %s, %s, %s, %s, %s)")

def time_change(time):
    if '/' in time:
        re_1 = time.split(' ')
        re_2 = re_1[0].split('/')
        return re_2[2] + '-' + re_2[0] + '-' + re_2[1] + ' ' + re_1[1]
    else:
        return time

if __name__ == '__main__':

     # data process
     df = pd.read_csv('./crawl data/reviews.csv', header=0)
     df = df.dropna()
     pd.set_option('display.max_columns', None)

     #
     df['date'] = df['date'].apply(lambda x:time_change(x))
     df['date'] = pd.to_datetime(df['date'])
     df['text'] = df['text'].str.strip()
     print(df.columns)
     #print(df['date'])

     # connet database
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

         for index, row in df.iterrows():

             data_reviews = (row['review id'], row['user id'], row['business id'], row['rating'], row['date'], row['text'])
             try:
                 cursor.execute(add_reviews, data_reviews)
             except:
                 print(data_business)


         cnx.commit()

         cursor.close()
         cnx.close()





