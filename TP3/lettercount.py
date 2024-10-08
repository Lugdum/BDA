from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class MRLetterCount(MRJob):
    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        for word in words:
            first_letter = word[0]
            if first_letter in string.ascii_lowercase:
                yield first_letter, 1

    def reducer(self, letter, counts):
        yield letter, sum(counts)

if __name__ == '__main__':
    MRLetterCount.run()
