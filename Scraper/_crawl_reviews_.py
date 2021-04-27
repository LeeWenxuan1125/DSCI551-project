import pandas as pd
import json
import requests
import sys
#Usage:
#   python _crawl_reviews_.py 0 2783
# 第一个参数是开始的行数，第二个参数是结束的行数



#url = 'https://api.yelp.com/v3/businesses/Ntwjgy-YzqW6of42nS1r0g/reviews'
url_base= 'https://api.yelp.com/v3/businesses/'
API_KEY= 'nT2yVBEbkSvPUNODNgeaErRg_psbcC_Z4Ph2G5nkk' \
         'LItgl02oJG7VPbcgDJfjILHdt8jUa6rddV0VYxaVw_HE_jobmuRQ5I_CUAe8_HXuMJO_81X7WXCH5pGqL0zYHYx'
headers = {'Authorization': 'Bearer %s' % API_KEY}

column_list = ["count","review id", "user id", "business id", "rating", "date", "text"]
df = pd.DataFrame(columns=column_list)

#read csv
df_cities = pd.read_csv('US businesses 50 states.csv')


count_cities = 0;
count_reviews = 0;


if __name__ == '__main__':
    if len(sys.argv)!=3:
        exit()

    start = int(sys.argv[1])
    end = int(sys.argv[2])
    df.to_csv('./reviews/reviews_yelp_' + str(sys.argv[1]) + '_' + str(sys.argv[2]) + '.csv', index=False, header=True)

    for index, row in df_cities.iloc[start: end].iterrows():

        count_cities = count_cities + 1
        print("count cities:", count_cities)
        #
        # if count_cities > sys.argv[1] and count_cities <= sys.argv[2]:
        #     pass
        # elif count_cities <= sys.argv[1]:
        #     continue
        # else:
        #     break
        print(row['business id'])
        print('--------------------------------')
        url = url_base +  row['business id'] + "/reviews"
        try:
            req = requests.get(url, headers=headers)
            parsed = json.loads(req.text)
            reviews = parsed['reviews']
        except:
            print("error occurs")
        else:
            for review in reviews:
                count_reviews = count_reviews + 1
                print("review id:", review['id'])
                print("user id:", review['user']["id"])
                print("business id", row['business id'])
                print("rating:", review['rating'])
                print("date:", review['time_created'])
                print("text:", review['text'])
                print('--------------------------------')
                review_dic = {
                    "count": count_reviews,
                    "review id": review['id'],
                    "user id": review['user']['id'],
                    "business id": row['business id'],
                    "rating": review["rating"],
                    "date": review["time_created"],
                    "text": review["text"]
                }
                df = df.append(pd.DataFrame.from_records([review_dic], columns=review_dic.keys()))

    print('Finishing!....')
    df.drop_duplicates(['review id'])
    df = df.set_index(['count'], drop=True)
    df.to_csv('./reviews/reviews_yelp_' + str(sys.argv[1]) + '_' +str(sys.argv[2]) + '.csv', index=False, header=True)