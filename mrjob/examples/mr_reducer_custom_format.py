from mrjob.job import MRJob

class MRReducerFinal(MRJob):

    def reducer(self, key, values):
        for value in values:
            self.stdout.write(bytes(len(value)))
            self.stdout.write(b' ')

if __name__ == '__main__':
    MRReducerFinal.run()
