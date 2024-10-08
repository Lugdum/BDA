from mrjob.job import MRJob

class MRTempDiff2(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        station_code = fields[0]
        measure_type = fields[2]
        value = int(fields[3])

        if measure_type in ['TMAX', 'TMIN']:
            yield station_code, (measure_type, value / 10.0)

    def reducer(self, station_code, values):
        sum_diff = 0
        count = 0
        temps = {}

        for measure_type, value in values:
            if measure_type not in temps:
                temps[measure_type] = []
            temps[measure_type].append(value)

        if 'TMAX' in temps and 'TMIN' in temps:
            for tmax, tmin in zip(temps['TMAX'], temps['TMIN']):
                sum_diff += tmax - tmin
                count += 1

            if count > 0:
                yield station_code, sum_diff / count

if __name__ == '__main__':
    MRTempDiff2.run()
