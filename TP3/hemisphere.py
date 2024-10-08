from mrjob.job import MRJob

class MRHemisphere(MRJob):
    def mapper(self, _, line):
        try :
            latitude = float(line[57:64])
            if latitude >= 0:
                yield 'north', 1
            elif latitude < 0:
                yield 'south', 1
        except:
            pass

    def reducer(self, hemisphere, count):
        yield hemisphere, sum(count)

if __name__ == '__main__':
    MRHemisphere.run()
