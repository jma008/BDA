import csv
import datetime
import pyspark
from pyspark.sql import SparkSession
import numpy as np
import json
import sys
import os

if __name__ == '__main__':
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    categories = {'452210': 'big_box_grocers', '452311': 'big_box_grocers', '445120': 'convenience_stores',
                  '722410': 'drinking_places', '722511': 'full_service_restaurants',
                  '722513': 'limited_service_restaurants', '446110': 'pharmacies_and_drug_stores',
                  '446191': 'pharmacies_and_drug_stores', '311811': 'snack_and_bakeries',
                  '722515': 'snack_and_bakeries', '445210': 'specialty_food_stores', '445220': 'specialty_food_stores',
                  '445230': 'specialty_food_stores', '445291': 'specialty_food_stores',
                  '445292': 'specialty_food_stores',
                  '445299': 'supermarkets_except_convenience_stores'}


    def countVisits(x):
        start = datetime.datetime.strptime(x[1][:10], "%Y-%m-%d")
        days = [start + datetime.timedelta(days=d) for d in range(8)]
        visits = json.loads(x[3])

        if x[1] == '2018-12-31T00:00:00-05:00':
            start = datetime.datetime(2019, 1, 1)
            days = [start + datetime.timedelta(days=d) for d in range(7)]
            visits = visits[1:]
        elif x[1] == '2020-12-28T00:00:00-05:00':
            start = datetime.datetime(2020, 12, 28)
            days = [start + datetime.timedelta(days=d) for d in range(4)]
            visits = visits[0:4]

        return tuple(zip([x[0]] * len(days), zip(days, visits)))


    def compute(x):
        row = lambda x: x if x >= 0 else 0

        # compute the median, low and high
        median = int(np.median(tuple(x[1])))
        stdev = int(np.std(tuple(x[1])))
        low = int(row(median - stdev))
        high = int(median + stdev)

        # project to the year 2020
        # get year and date
        year = int(x[0][1].year)
        date = str(e[0][1].replace(2020).date())

        return x[0][0], year, date, median, low, high


    # user-added argument output folder path
    OUTPUT_PREFIX = sys.argv[1]

    rdd = sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')

    # creating a dictionary of safegraph place ids for filtering on weekly patterns
    ids = dict(
        rdd.map(lambda x: next(csv.reader([x.encode('utf-8')])))
            .filter(lambda x: x[9] in categories.keys())
            .map(lambda x: (x[1], x[9]))
            .collect())

sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*') \
        .map(lambda x: next(csv.reader([x.encode('utf-8')]))) \
        .filter(lambda x: x[1] in ids.keys()) \
        .map(lambda x: (categories.get(ids.get(x[1])), x[12], x[13], x[16])) \
        .filter(lambda x: x[1] >= '2018-12-31T00:00:00-05:00' and x[2] <= '2021-01-04T00:00:00-05:00') \
        .map(countVisits) \
        .flatMap(lambda x: x) \
        .map(lambda x: ((x[0], x[1][0]), x[1][1])) \
        .groupByKey() \
        .map(countVisits) \
        .toDF() \
        .write \
        .partitionBy('_1') \
        .option('header', 'true') \
        .csv(OUTPUT_PREFIX)
for dir in os.listdir(OUTPUT_PREFIX):
    os.rename(OUTPUT_PREFIX + '/' + dir, OUTPUT_PREFIX + '/' + dir.replace('_1=', ''))
