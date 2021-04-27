import pandas as pd
import requests
import json


url = 'https://api.yelp.com/v3/businesses/search'
API_KEY= 'kZZCyM6sVJiWmAnCeidZbgMc1MnyGZh' \
         'SzI43svbeYzhGeIUQ0UwYO-JyfUro8useaE0yIRcjzeDiMqQhlWefvpI3pNJTslxMmxrE5gta4y2nAjM1jpWN0vnUJ_QlYHYx'

DEFAULT_TERM = 'restaurant'
DEFAULT_LOCATION = 'Ozark，Alabama'
count = 0
column_list = ["count", "Restaurant name", "rating score", "review count", "city", "state", "business id", "latitude",
               "longitude","categories"]
df = pd.DataFrame(columns=column_list)


if __name__ == '__main__':

    #read xlsx file and remove duplicated according to City column
    US_cities = pd.read_excel('us-cities-demographics.xlsx')
    US_cities = US_cities.drop_duplicates(['City'])

    #header
    headers = {'Authorization': 'Bearer %s' % API_KEY}


    for index, row in US_cities.iterrows():

        location = row['City'] + ', ' + row['State Code']
        print('------' + location + '------')
        for j in range(20):
            params = {'term': 'restaurant', 'location': location, "limit": 50, "offset": 50*j}
            try:
                req = requests.get(url, params=params, headers=headers)
                parsed = json.loads(req.text)
                businesses = parsed["businesses"]
            except:
                break;
            else:
                for business in businesses:
                    count += 1
                    print('--------------------------------')
                    print(count)
                    print("Name:", business["name"])
                    print("Rating:", business["rating"])
                    print("review count:", business['review_count'])
                    print("City:", "".join(business["location"]["city"]))
                    print("State:", "".join(business["location"]["state"]))
                    print("Address:", " ".join(business["location"]["display_address"]))
                    print("Business id:", business["id"])
                    print("Latitude:", business['coordinates']['latitude'])
                    print("Longitude:", business['coordinates']['longitude'])

                    category_lst = ' '
                    if len(business['categories']) > 0:
                        for k in range(len(business['categories']) - 1):
                            # print("Categories:",business['categories'][])
                            # category_lst.append(business['categories'][k]["title"])
                            category_lst += business['categories'][k]["title"] + ", "
                        category_lst += business['categories'][len(business['categories']) - 1]["title"]
                    else:
                        category_lst = " "
                    print("Categories:", category_lst)
                    business_dict = {
                        "count": count,
                        "Restaurant name": business["name"],
                        "rating score": business["rating"],
                        "review count": business['review_count'],
                        "city": business["location"]["city"],
                        "state": business["location"]["state"],
                        "business id": business["id"],
                        "latitude": business['coordinates']['latitude'],
                        "longitude": business['coordinates']['longitude'],
                        # "transaction":business['transactions'],
                        "categories": category_lst
                    }
                    df = df.append(pd.DataFrame.from_records([business_dict], columns=business_dict.keys()))

                if 50*(j+1) > parsed['total']:
                    break;

    print('Finishing!....')
    print(count)
    df.drop_duplicates(['business id'])
    df = df.set_index(['count'], drop=True)
    df.to_csv('export_dataframe_lee.csv', index=False, header=True)

