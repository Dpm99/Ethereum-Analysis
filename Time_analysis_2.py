from mrjob.job import MRJob
import time

class PartA2(MRJob):

    def mapper(self, _, line):

        try:
            fields = line.split(',')
            if (len(fields)== 7):

                time_epoch = int(fields[6])
                date = time.strftime("%Y%m",time.gmtime(time_epoch))## Gives the year and the month
                value = int(fields[3])
                yield(date, (value,1))

        except:
            pass


    def combiner(self, date, values):
        count = 0
        total = 0
        for value in values:

            count+= value[1]
            total+= value[0]

        yield(date, (total, count))

    def reducer(self, date, values):
        count = 0
        total = 0
        for value in values:
            count+=value[1]
            total+=value[0]
        yield(date, total/count)


if __name__=='__main__':
    PartA2.run()