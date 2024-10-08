from mrjob.job import MRJob

class MRTempDiff(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        station_code = fields[0]
        measure_type = fields[2]
        value = int(fields[3])

        if measure_type in ['TMAX', 'TMIN']:
            yield station_code, (measure_type, value / 10.0)

    def reducer(self, date, values):
        max_temp = None
        min_temp = None

        for measure_type, value in values:
            if measure_type == 'TMAX':
                if max_temp is None:
                    max_temp = value
                else:
                    max_temp = max(max_temp, value)
            elif measure_type == 'TMIN':
                if min_temp is None:
                    min_temp = value
                else:
                    min_temp = min(min_temp, value)

        if max_temp is not None and min_temp is not None:
            yield date, max_temp - min_temp

if __name__ == '__main__':
    MRTempDiff.run()
