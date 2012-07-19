from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class FilterJob(MRJob):

    INPUT_PROTOCOL = RawValueProtocol

    INTERNAL_PROTOCOL = RawValueProtocol

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(FilterJob, self).configure_options()
        self.add_passthrough_option('--mapper-filter', default=None)
        self.add_passthrough_option('--combiner-filter', default=None)
        self.add_passthrough_option('--reducer-filter', default=None)

    def steps(self):
        kwargs = {}
        if self.options.mapper_filter:
            kwargs['mapper_pre_filter'] = self.options.mapper_filter
        if self.options.combiner_filter:
            kwargs['combiner_pre_filter'] = self.options.combiner_filter
        if self.options.reducer_filter:
            kwargs['reducer_pre_filter'] = self.options.reducer_filter
        return [self.mr(**kwargs)]


if __name__ == '__main__':
    FilterJob().run()
