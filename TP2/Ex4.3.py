from mrjob.job import MRJob

class MRLongestWordPerLine(MRJob):
    def mapper(self, _, line):
        longest_word = ""
        words = line.split()
        for word in words:
            if len(word) > len(longest_word):
                longest_word = word
        yield None, longest_word

if __name__ == '__main__':
    MRLongestWordPerLine.run()