from flask import Flask, render_template, request, flash
from yelpEmailjdbc import total_report, sendEmail
from pyspark.shell import sqlContext, spark

app = Flask(__name__)
app.secret_key = "DSCI551"


business = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/yelp").option("driver","com.mysql.cj.jdbc.Driver")\
   .option("dbtable", "businesses").option("user", "root").option("password", "Umtruelover1005").load()
review =  spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/yelp").option("driver","com.mysql.cj.jdbc.Driver")\
   .option("dbtable", "reviews").option("user", "root").option("password", "Umtruelover1005").load()

sta_df = total_report(business, review)


@app.route('/', methods = ['GET', 'POST'])
def index():
   if request.method == 'POST':

      email = request.form.get('Email')
      state = request.form.get('state')

      if all([email, state]):
         print(email)
         print(state)
         sendEmail(sta_df, state, email)
         flash(u"success")  # flash 需要对内容加密，因此需要设置secret-key


   return render_template('yelp.html')



if __name__ == '__main__':
   app.run()
