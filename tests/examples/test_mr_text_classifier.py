# Copyright 2019 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from glob import glob
from os.path import dirname
from os.path import join

import mrjob
from mrjob.examples.mr_text_classifier import MRTextClassifier
from mrjob.examples.mr_text_classifier import parse_doc_filename
from mrjob.protocol import StandardJSONProtocol

from tests.job import run_job
from tests.sandbox import BasicTestCase
from tests.py2 import patch


class ParseDocFileNameTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(
            parse_doc_filename(''),
            dict(id='', cats=dict()))

    def test_no_cats(self):
        self.assertEqual(
            parse_doc_filename('uncategorized.txt'),
            dict(id='uncategorized', cats=dict()))

    def test_with_cats(self):
        self.assertEqual(
            parse_doc_filename('the_christening-milne-animals.txt'),
            dict(id='the_christening', cats=dict(animals=True, milne=True)))

    def test_not_cat(self):
        self.assertEqual(
            parse_doc_filename('buckingham_palace-milne-not_america.txt'),
            dict(id='buckingham_palace', cats=dict(america=False, milne=True)))


class MRTextClassifierTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(
            run_job(MRTextClassifier([])),
            {('doc', ''): dict(
                cats={}, cat_to_score={}, id='', in_test_set=True)})

    def test_can_tell_milne_from_whitman(self):
        docs_paths = glob(join(
            dirname(mrjob.__file__), 'examples', 'docs-to-classify', '*'))

        # use --min-df 1 because we have so few documents
        job_args = ['--min-df', '1'] + docs_paths

        output = run_job(MRTextClassifier(job_args))
        test_set_docs = [
            doc for k, doc in output.items()
            if k[0] == 'doc' and not doc['in_test_set']
        ]

        # make sure that there are some docs in the test set
        self.assertGreater(len(test_set_docs), 3)

        for doc in test_set_docs:
            for cat in ('milne', 'whitman'):
                # include doc ID to make debugging easier
                self.assertEqual(
                    (doc['id'], bool(doc['cats'].get(cat))),
                    (doc['id'], bool(doc['cat_to_score'][cat] > 0)))

        # the empty doc should only be something that appears with no input
        self.assertNotIn(('doc', ''), output)

    def test_works_with_built_in_json_module(self):
        # regression test: make sure we're not trying to serialize dict_items
        self.start(patch.object(MRTextClassifier,
                                'INTERNAL_PROTOCOL', StandardJSONProtocol))
        self.start(patch.object(MRTextClassifier,
                                'OUTPUT_PROTOCOL', StandardJSONProtocol))

        docs_paths = glob(join(
            dirname(mrjob.__file__), 'examples', 'docs-to-classify', '*'))

        # use --min-df 1 because we have so few documents
        job_args = ['--min-df', '1'] + docs_paths

        run_job(MRTextClassifier(job_args))
