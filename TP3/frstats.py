from mrjob.job import MRJob

class MRFRStats(MRJob):

    def mapper(self, _, line):
        country = line[43:45]
        if country == "FR":
            try:
                station = line[13:42].strip()
                latitude = float(line[57:64].strip())
                elevation = float(line[74:81].strip())

                yield "south", (latitude, station)
                yield "north", (latitude, station)
                yield "altitude", (elevation, station)
            except:
                pass

    def reducer(self, key, values):
        if key == "south":
            station = min(values)
        elif key == "north":
            station = max(values)
        else:
            station = max(values)

        yield key, station[1]

if __name__ == '__main__':
    MRFRStats.run()
