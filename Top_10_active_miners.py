import pyspark

sc = pyspark.SparkContext()


def good_line_mine(line):

    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False

        int(fields[4])
        return True
    except:
        return False

lines = sc.textFile('/data/ethereum/blocks')

clean_lines = lines.filter(good_line_mine)
features = clean_lines.map(lambda x: (x.split(',')[2], int(x.split(',')[4])))

join_features = features.reduceByKey(lambda a,b:a+b)

top_10 = join_features.takeOrdered(10, key = lambda x: -x[1])

for record in top_10:

    print('{}: {}'.format(record[0], record[1]))
