import numpy as np
import pandas as pd
import sys
from operator import add
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.shell import sqlContext, spark
import pyspark.sql.functions as fc
import mysql.connector
import sys
from pyspark.sql.types import FloatType
import statistics
from statistics import mean, median, mode, stdev
import matplotlib.pyplot as plt
import functools
import operator
import smtplib
import email
import smtplib
from email.mime.text import MIMEText
from email.header import Header

business = spark.read.csv('business dataset.csv')
review = spark.read.csv('reviews.csv')

def total_report(business, review):

    #state_name_input = sys.argv[1]
    business = business.na.drop()
  #  business = business.selectExpr("_c0 as c0","_c1 as restaurant_name","_c2 as rating_score","_c3 as review_count","_c4 as city","_c5 as business_id","_c6 as latitude","_c7 as longitude","_c8 as categories","_c9 as State","_c10 as state_code")

    spark_df = business.groupBy('state_name').agg(fc.mean('score').alias('avg_rating'),fc.max('score').alias('max_rating'),fc.min('score').alias('min_rating'),
    fc.count('*').alias('restaurant_count')).orderBy(fc.desc('restaurant_count'))

    favorite_df = business[(business.score>4.9)].dropDuplicates().groupBy('state_name').agg(fc.countDistinct('categories').alias('categories_count'),
    fc.count('*').alias('restaurant_count')).orderBy(fc.desc('restaurant_count'))

    avg_rating_lst = list(spark_df.select('avg_rating').toPandas()['avg_rating'])
    max_rating_lst = list(spark_df.select('max_rating').toPandas()['max_rating'])
    min_rating_lst = list(spark_df.select('min_rating').toPandas()['min_rating'])
    restaurant_count_lst = list(spark_df.select('restaurant_count').toPandas()['restaurant_count'])
    category_count_lst = list(favorite_df.select('categories_count').toPandas()['categories_count'])



    import_db = mysql.connector.connect(host='localhost',user='root',password='Umtruelover1005',database='yelp')
    mycursor = import_db.cursor(buffered=True)
    mycursor.execute('select state_name, count(review_id) from businesses join reviews on businesses.id = reviews.business_id group by state_name')
    result = mycursor.fetchall()


    def most_frequent(List):
        counter = 0
        num = List[0]

        for i in List:
            curr_frequency = List.count(i)
            if (curr_frequency > counter):
                counter = curr_frequency
                num = i

        return num



    def convertTuple(tup):
        str = functools.reduce(operator.add, (tup))
        return str



    sql_query = pd.read_sql_query('select * from businesses',import_db)
    business_df = pd.DataFrame(sql_query,columns = ['restaurant_name','score','review_num','city','id','latitude','longitude','categories','state_name','state_code'])

    state_notunique = list(business_df['state_name'])


    def unique(list1):
        unique_list = []
        for x in list1:
            if x not in unique_list:
                unique_list.append(x)
        return unique_list
    state = unique(state_notunique)

    state_code_not_unique = list(business_df['state_code'])
    state_code = unique(state_code_not_unique)

    def most_popular_category(state_code):
        new_lst = []
        for i in range(len(state_code)):
            state_code_1 = state_code[i]
            mycursor3 = import_db.cursor(buffered=True)
            mycursor3.execute("select distinct categories from businesses where score =5.0 and state_code = '{}'".format(state_code_1))
            result3 = mycursor3.fetchall()
            most_frequent_category = most_frequent(result3)
            str_most_frequent_category = convertTuple(most_frequent_category)
            new_lst.append(str_most_frequent_category)
        return new_lst

    most_popular_category_lst = most_popular_category(state_code)



    median_list = []
    mode_list = []
    stdev_list = []
    first_quantile_list = []
    third_quantile_list = []
    column_list = ["count","state_name","avg_score","max_score","min_score","restaurant_count","category_count","most_favorite_dishes_type","Median","Mode","standard deviation","First quantile",'Third quantile']
    statistics_df = pd.DataFrame(columns=column_list)
    count = 1
    for i in range(len(state)):
        median_list.append(statistics.median(business_df['score'][business_df['state_name'] == state[i]]))
        mode_list.append(statistics.mode(business_df['score'][business_df['state_name'] == state[i]]))
        stdev_list.append(round(statistics.stdev(business_df['score'][business_df['state_name'] == state[i]]),2))
        first_quantile_list.append(np.quantile((business_df['score'][business_df['state_name'] == state[i]]),0.25))
        third_quantile_list.append(np.quantile((business_df['score'][business_df['state_name'] == state[i]]),0.75))
        dict_ = {
            "count":count,
            "state_name": state[i],
            "avg_score": avg_rating_lst[i],
            "max_score": max_rating_lst[i],
            "min_score": min_rating_lst[i],
            "restaurant_count": restaurant_count_lst[i],
            "category_count": category_count_lst[i],
            "most_favorite_dishes_type": most_popular_category_lst[i],
            "Median": median_list[i],
            "Mode": mode_list[i],
            "standard deviation": stdev_list[i],
            "First quantile": first_quantile_list[i],
            "Third quantile":third_quantile_list[i]
                }
        count += 1
        statistics_df = statistics_df.append(pd.DataFrame.from_records([dict_], columns=dict_.keys()))

    statistics_df = statistics_df.set_index(['count'],drop = True)
    return statistics_df


def report(statistics_df,parameter):

    state_stat = statistics_df.loc[statistics_df['state_name'] == parameter]
    state_avg_score = state_stat['avg_score']
    str_state_avg_score = state_avg_score.to_string()

    state_max_score = state_stat['max_score']
    str_state_max_score = state_max_score.to_string()

    state_min_score = state_stat['min_score']
    str_state_min_score = state_min_score.to_string()

    state_restaurant_count = state_stat['restaurant_count']
    str_state_restaurant_count = state_restaurant_count.to_string()

    state_category_count = state_stat['category_count']
    str_state_category_count = state_category_count.to_string()

    state_category = state_stat['most_favorite_dishes_type']
    str_state_category = state_category.to_string()

    state_median_score = state_stat['Median']
    str_state_median_score = state_median_score.to_string()

    state_mode_score = state_stat['Mode']
    str_state_mode_score = state_mode_score.to_string()

    state_sd_score = state_stat['standard deviation']
    str_state_sd_score = state_sd_score.to_string()

    state_1st_score = state_stat['First quantile']
    str_state_1st_score = state_1st_score.to_string()

    state_3st_score = state_stat['Third quantile']
    str_state_3st_score = state_3st_score.to_string()

    a = 'Below is statistics report for restaurants in ' + str(parameter) +'\n\n'
    b = 'The average score of restaurants in entire ' + str(parameter) + ' is ' + str_state_avg_score[11:] + '\n'
    c = 'The max score of restaurants in entire ' + str(parameter) + ' is ' + str_state_max_score[11:] + '\n'
    d = 'The min score of restaurants in entire ' + str(parameter) + ' is ' + str_state_min_score[11:] + '\n'
    e = 'The total number of restaurants in entire ' + str(parameter) + ' is ' + str_state_restaurant_count[
                                                                                 11:] + '\n'
    f = 'The distinct number of type of dishes of restaurants in entire ' + str(
        parameter) + ' is ' + str_state_category_count[11:] + '\n'
    g = 'The most popular of type of dishes of restaurants in entire ' + str(
        parameter) + ' is ' + str_state_category[11:] + '\n'
    h = 'The median score of restaurants in entire ' + str(parameter) + ' is ' + str_state_median_score[11:] + '\n'
    i = 'The mode score of restaurants in entire ' + str(parameter) + ' is ' + str_state_mode_score[11:] + '\n'
    j = 'The standard deviation of score in restaurants in entire ' + str(parameter) + ' is ' + str_state_sd_score[
                                                                                                11:] + '\n'
    k = 'The first quantile of score in restaurants in entire ' + str(parameter) + ' is ' + str_state_1st_score[
                                                                                            11:] + '\n'
    l = 'The third quantile of score in restaurants in entire ' + str(parameter) + ' is ' + str_state_3st_score[
                                                                                            11:] + '\n'
    total = ""
    total += a
    total += b
    total += c
    total += d
    total += e
    total += f
    total += g
    total += h
    total += i
    total += j
    total += k
    total += l

    return total

def sendEmail(statistics_df, state_name_input, email_input):
    my_report = report(statistics_df, state_name_input)

    from_addr = '630191748@qq.com'
    password = 'qzjgmxdmkrvxbehf'
    to_addr = email_input

    smtp_server = 'smtp.qq.com'
    msg = MIMEText(report(statistics_df, state_name_input), 'plain', 'utf-8')

    msg['From'] = Header(from_addr)
    msg['To'] = Header(to_addr)
    msg['Subject'] = Header("yelp report content")

    server = smtplib.SMTP_SSL(smtp_server)
    server.connect(smtp_server, 465)
    server.login(from_addr, password)
    server.sendmail(from_addr, to_addr, msg.as_string())
    server.quit()

if __name__ == '__main__':

    state_name_input =  "Alaska"
    email_input = "15728012845@163.com"
    sta_df = total_report(business, review)
    sendEmail(sta_df, state_name_input, email_input)

