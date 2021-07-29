from mrjob.job import MRJob
import time

class PartA(MRJob):

    def mapper(self, _, line):

        try:
            fields = line.split(',')
            if (len(fields)== 7):

                time_epoch = int(fields[6])
                date = time.strftime("%Y%m",time.gmtime(time_epoch))## Gives the year and the month
                yield(date, 1)
        except:
            pass

    def reducer(self, date, counts):
        yield(date, sum(counts))


if __name__=='__main__':
    PartA.run()