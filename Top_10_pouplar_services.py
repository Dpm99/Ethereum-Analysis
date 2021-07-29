import pyspark

sc = pyspark.SparkContext()


def good_line_transactions(line):

    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        int(fields[3])
        return True
    except:
        return False

def good_line_contracts(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False
        else:
            return True
    except:
        return False


lines_1 = sc.textFile('/data/ethereum/transactions')
clean_lines_1 = lines_1.filter(good_line_transactions)
features_1 = clean_lines_1.map(lambda x: (x.split(',')[2], int(x.split(',')[3])))
sum_features_1 = features_1.reduceByKey(lambda a,b:a+b) ## sum the transactions of the same address

lines_2 = sc.textFile('/data/ethereum/contracts')

clean_lines_2 = lines_2.filter(good_line_contracts)
features_2 = clean_lines_2.map(lambda y: (y.split(',')[0],1)) ## get contract adresses


join_features = features_2.join(sum_features_1) ## join the two features_1

## element in join features = (address, (address, value))

filter_features = join_features.map(lambda i: (i[0], i[1][1]))
top_10 = filter_features.takeOrdered(10, key = lambda k: -1*k[1]) ##-val2 because we want the biggest

#print(top_10)
for record in top_10:
    print('{}: {}'.format(record[0], record[1]))
