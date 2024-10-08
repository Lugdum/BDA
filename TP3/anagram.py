from mrjob.job import MRJob

class MRAnagramFinder(MRJob):
    def mapper(self, _, line):
        line = line.lower()
        words = line.split()
        for word in words:
            if len(word) > 1 and word.isalpha():
                sorted_letters = ''.join(sorted(word))
                yield sorted_letters, word

    def reducer(self, sorted_letters, words):
        unique_words = set(words)
        if len(unique_words) > 1:
            yield sorted_letters, list(unique_words)

if __name__ == '__main__':
    MRAnagramFinder.run()
