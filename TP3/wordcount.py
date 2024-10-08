from mrjob.job import MRJob
import re

match_word = re.compile(r"[\w']+")

class MRSameWorkFrequency(MRJob):
    def mapper(self, _, line):
        words = match_word.findall(line.lower())
        for word in words:
            yield word, 1
    
    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRSameWorkFrequency.run()
