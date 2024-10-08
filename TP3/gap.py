from mrjob.job import MRJob

class MRGap(MRJob):

    def mapper(self, _, line):
        country = line[43:45]
        try:
            station = line[13:42].strip()
            begin = int(line[82:90])
            end = int(line[91:99])
            gap = begin - end

            yield country, (gap, station)
        except:
            pass

    def reducer(self, key, values):
        max_gap_station = max(values, key=lambda x: (x[0], -len(x[1])))
        yield key, max_gap_station[1]

if __name__ == '__main__':
    MRGap.run()
