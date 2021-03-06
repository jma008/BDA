import csv
import datetime
import json
import numpy as np
import pyspark
import os
import sys



def main(file):
    sc = pyspark.SparkContext()
    core_places = 'hdfs:///data/share/bdm/core-places-nyc.csv'
    weekly_pattern = 'hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*'
    output_prefix = file
    # create a directory
    os.mkdir(output_prefix)

    categories = {'big_box_grocers': ['452210', '452311'], 'convenience_stores': ['445120'],
                  'drinking_places': ['722410'], 'full_service_restaurants': ['722511'],
                  'limited_service_restaurants': ['722513'], 'pharmacies_and_drug_stores': ['446110', '446191'],
                  'snack_and_bakeries': ['311811', '722515'],
                  'specialty_food_stores': ['445210', '445220', '445230', '445291', '445292', '445299'],
                  'supermarkets_except_convenience_stores': ['445110']}

    def naics_code(partId, records):
        if partId == 0:
            next(records)

        reader = csv.reader(records)
        valid_codes = ['722511', '722513', '452210', '452311', '445120', '722410', '446110', '446191', '311811',
                       '722515', '445210', '445220', '445230', '445291', '445292', '445299', '445110']
        for row in reader:
            if row[9] in valid_codes:
                # get place key and naics code
                yield (row[1], row[9])

    places = sc.textFile(core_places, use_unicode= False).cache()
    valid_places = places.mapPartitionsWithIndex(naics_code)

    def extract_visits(partId, records):
        if partId == 0:
            next(records)
        reader = csv.reader(records)

        for row in reader:
            yield (row[1], (row[12], row[13], row[16]))

    restaurants = sc.textFile(weekly_pattern,use_unicode=False).cache()
    mapped_restaurant_records = restaurants.mapPartitionsWithIndex(extract_visits)



    def calculations(x):
        values = x[1]
        year = x[0][:4]
        sorted_values = sorted(list(values))
        middle = len(sorted_values) // 2
        median = (sorted_values[middle] + sorted_values[~middle]) / 2
        stdev = np.std(sorted_values)
        low = max(0, median - stdev)
        high = median + stdev
        return (year, x[0], low, median, high)

    def expand_rows(_, iterator):
        for row in iterator:
            start_day = row[1][1][0][:10]
            end_day = row[1][1][1][:10]
            visits = json.loads(row[1][1][2])

            for day in range(0, 7):
                date = datetime.datetime.strptime(start_day, "%Y-%m-%d") + datetime.timedelta(days=day)
                yield (date.strftime("%Y-%m-%d"), visits[day])

    for k, v in categories.items():
        limited_service_restaurants = valid_places.join(mapped_restaurant_records).filter(lambda x: x[1][0] in v)
        day_visits = limited_service_restaurants.mapPartitionsWithIndex(expand_rows)
        final_values = day_visits.groupByKey().map(calculations)
        sp = pyspark.sql.SparkSession(sc)
        if final_values.isEmpty():
            continue
        df = sp.createDataFrame(data=final_values).toDF("year", "date", "low", "median", "high")
        df = df.orderBy("date")
        df.show()
        df.write.csv("{}/{}".format(output_prefix,k))


if __name__ == "__main__":
    file = sys.argv[1]
    main(file)
