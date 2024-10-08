from mrjob.job import MRJob

class MRAvrage(MRJob):
    def mapper(self, _, line):
        year, month, temperature = line.split(',')
        yield year, temperature

    def reducer(self, year, temperatures):
        total_temp = 0
        num = 0
        for temp in temperatures:
            total_temp += int(temp)
            num += 1
        yield year, total_temp / num

if __name__ == '__main__':
    MRAvrage.run()
