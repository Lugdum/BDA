from mrjob.job import MRJob

class MRSameWorkFrequency(MRJob):
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word, 1
    
    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRSameWorkFrequency.run()
