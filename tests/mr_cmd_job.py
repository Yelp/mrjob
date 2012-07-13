from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


class CmdJob(MRJob):

    INPUT_PROTOCOL = RawValueProtocol

    INTERNAL_PROTOCOL = RawValueProtocol

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(CmdJob, self).configure_options()
        self.add_passthrough_option('--mapper-cmd', default=None)
        self.add_passthrough_option('--combiner-cmd', default=None)
        self.add_passthrough_option('--reducer-cmd', default=None)

    def steps(self):
        kwargs = {}
        if self.options.mapper_cmd:
            kwargs['mapper_cmd'] = self.options.mapper_cmd
        if self.options.combiner_cmd:
            kwargs['combiner_cmd'] = self.options.combiner_cmd
        if self.options.reducer_cmd:
            kwargs['reducer_cmd'] = self.options.reducer_cmd
        return [self.mr(**kwargs)]


if __name__ == '__main__':
    CmdJob().run()
