from mrjob.job import MRJob

class MRPalindromeFinder(MRJob):
    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        for word in words:
            if len(word) > 1 and word.isalpha():
                if word == word[::-1]:
                    yield word, 1

    def reducer(self, word, words):
        yield word, sum(words)

if __name__ == '__main__':
    MRPalindromeFinder.run()
