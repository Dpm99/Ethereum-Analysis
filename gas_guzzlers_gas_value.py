from mrjob.job import MRJob
import time

class gas_average(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            time_epoch = int(fields[6])
            if time_epoch != 0:
                date = time.strftime("%m-%Y", time.gmtime(time_epoch))
                value = int(fields[4])
            yield(date, (value,1))
        except:
            pass

    def reducer(self, key, values):
        count = 0
        total = 0
        for value in values:
            count += value[1]
            total += value[0]
        yield(key, total/count)

if __name__ == '__main__':
    gas_average.run()