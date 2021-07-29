import pyspark
import time
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


data1 = clean_lines_1.map(lambda x: (time.strftime("%Y-%m",time.gmtime(int(x.split(',')[6]))), 1)) ## (date, 1)
data2 = clean_lines_1.map(lambda y: (time.strftime("%Y-%m",time.gmtime(int(y.split(',')[6]))), int(y.split(',')[5]))) ## (date, price)

sum_dates = data1.reduceByKey(lambda k,j: k+j) ## counts the transactions in a month
sum_prices = data2.reduceByKey(lambda f,g: f+g) ## sums the price in a month


join_features = sum_dates.join(sum_prices)


## (date, (counts, sum_prices))

final_features = join_features.map(lambda i: (i[0], float(i[1][1]/i[1][0]))) ## (date, average)
final_features.saveAsTextFile('gas_price_per_month')