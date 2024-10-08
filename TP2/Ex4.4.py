from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTopLongestWords(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_collect_words,
                   reducer=self.reducer_get_top_words)
        ]

    def mapper_get_words(self, _, line):
        for word in line.split():
            yield None, (len(word), word)

    def combiner_collect_words(self, _, word_lengths):
        sorted_words = sorted(word_lengths, key=lambda wl: (-wl[0], wl[1]))[:10]
        for word_length in sorted_words:
            yield None, word_length

    def reducer_get_top_words(self, _, word_lengths):
        sorted_words = sorted(word_lengths, key=lambda wl: (-wl[0], wl[1]))[:10]
        yield "Le mot le plus long", sorted_words[0][1]
        yield "Top 10 des mots les plus longs", [wl[1] for wl in sorted_words]

if __name__ == '__main__':
    MRTopLongestWords.run()
