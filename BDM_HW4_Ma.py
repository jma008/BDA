import csv
import datetime
import functools
import json
import statistics
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

if __name__ == '__main__':
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    categories = {'Big Box Grocers': {'452210', '452311'},
                  'Convenience Stores': {'445120'},
                  'Drinking Places': {'722410'},
                  'Full-Service Restaurants': {'722511'},
                  'Limited-Service Restaurants': {'722513'},
                  'Pharmacies and Drug Stores': {'446110', '446191'},
                  'Snack and Bakeries': {'311811', '722515'},
                  'Specialty Food Stores': {'445210', '445220', '445230', '445291', '445292', '445299'},
                  'Supermarkets (except Convenience Stores)': {'445110'}}

    for category in list(categories.keys()):
        dfBBG = spark.read.csv('hdfs:///data/share/bdm/core-places-nyc.csv', header=True, escape='"') \
            .filter(F.col('naics_code') \
                    .isin(*categories[category])) \
            .select('safegraph_place_id')


        def Year(date):
            return int(date.year)


        def Date(date):
            return datetime.date(2020, date.month, date.day)


        def expandVisits(date_range_start, visits_by_day):
            start = datetime.date(*map(int, date_range_start[:10].split('-')))
            visits = json.loads(visits_by_day)
            return {start + datetime.timedelta(days=d): cnt
                    for d, cnt in enumerate(visits)}


        def computeMedian(totalCount, visits):
            if len(visits) < totalCount:
                visits += [0] * (totalCount - len(visits))
            return int(statistics.median(visits) + 0.5)


        def computeLow(totalCount, visits):
            if len(visits) < totalCount:
                visits += [0] * (totalCount - len(visits))
            stdev = statistics.stdev(visits) + 0.5
            median = statistics.median(visits) + 0.5
            if median <= stdev:
                return 0
            else:
                return int(median - stdev)


        def computeHigh(totalCount, visits):
            if len(visits) < totalCount:
                visits += [0] * (totalCount - len(visits))
            stdev = statistics.stdev(visits) + 0.5
            median = statistics.median(visits) + 0.5
            return int(median + stdev)


        udfYear = F.udf(Year, T.IntegerType())
        udfDate = F.udf(Date, T.DateType())
        udfExpand = F.udf(expandVisits, T.MapType(T.DateType(), T.IntegerType()))
        udfMedian = F.udf(functools.partial(computeMedian, dfBBG.count()), T.IntegerType())
        udfLow = F.udf(functools.partial(computeLow, dfBBG.count()), T.IntegerType())
        udfHigh = F.udf(functools.partial(computeHigh, dfBBG.count()), T.IntegerType())

        OUTPUT_PREFIX = sys.argv[1]

        spark.read.csv('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"') \
            .join(dfBBG, 'safegraph_place_id') \
            .select(F.explode(udfExpand('date_range_start', 'visits_by_day')) \
                    .alias('date', 'visits')) \
            .groupBy('date') \
            .agg(F.collect_list('visits').alias('visits')) \
            .select(udfYear('date').alias('year'),
                    udfDate('date').alias('date'),
                    udfMedian('visits').alias('median'),
                    udfLow('visits').alias('low'),
                    udfHigh('visits').alias('high')) \
            .sort('date') \
            .coalesce(1) \
            .write \
            .partitionBy('_1') \
            .option('header', 'true') \
            .csv(OUTPUT_PREFIX)

        for dirname in os.listdir(OUTPUT_PREFIX):
            os.rename(OUTPUT_PREFIX + '/' + dirname, OUTPUT_PREFIX + '/' + dirname.replace('_1=', ''))
