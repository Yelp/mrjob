# Copyright 2009-2013 Yelp and Contributors
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

"""Write and run Hadoop Streaming jobs on Amazon Elastic MapReduce or your own
Hadoop cluster.
"""

from .version import __version__

__author__ = 'David Marin <dm@davidmarin.org>'

__credits__ = [
    'Jordan Andersen <jordandandersen@gmail.com>',
    'Tom Arnfeld <tarnfeld@me.com>',
    'Martin Baeuml <baeuml@kit.edu>',
    'Hunter Blanks <hblanks@monetate.com>',
    'Jim Blomo <jblomo@yelp.com>',
    'Reno Bowen <renobowen@gmail.com>',
    'James Brown <jbrown@uber.com>',
    'Kevin Burke <kevin@twilio.com>',
    'Ewen Cheslack-Postava <me@ewencp.org>',
    'Jordan Christensen <jc@kobo.com>',
    'David Dehghan <ddehghan@gmail.com>',
    'Adam Derewecki <derewecki@gmail.com>',
    'Nick Dimiduk <ndimiduk@gmail.com>',
    'Tom Dooner <tomdooner@gmail.com>',
    'Dan Frank <danielhfrank@gmail.com>',
    'Benjamin Goldenberg <benjamin@yelp.com',
    'Buck Golemon <buck@yelp.com>',
    'Peter Harrington <peter.b.harrington@gmail.com>',
    'Brandon Haynes <bhaynes@fas.harvard.edu>',
    'Tim Henderson <tim.tadh@gmail.com>',
    'Tom Hennigan <tomhennigan@gmail.com>',
    'Brett Hoerner <brett@bretthoerner.com>',
    'Evan Klitzke <evan@eklitzke.org>',
    'Tom Janofsky <tjanofsky@monetate.com>',
    'Stephen Johnson <steve@steveasleep.com>',
    'Matt Jones <matt@mhjones.org>',
    'Kamil Kisiel <kamil@kamilkisiel.net>',
    'Nikolaos Koutsopoulos <nhk@mochimedia.com>',
    'Julian Krause <juliank@yelp.com>',
    'Robert Leftwich <rl.0x0@eml.cc>',
    'Baris Metin <bmetin@yelp.com>',
    'Tetsuya Morimoto <tetsuya.morimoto@gmail.com>',
    'Daniel Nephin <dnephin@yelp.com>',
    'Sean Myers <seanmyers0608@gmail.com>',
    'Oliver Nicholas <bigo@wonlove.net>',
    'Matt Perry <matt@unshift.net>',
    'Kien Pham <kien@sendgrid.com>',
    'Andrew Price <andrew.price@ensighten.com>',
    'Pavel Repin <prepin@gmail.com>',
    'Wahbeh Qardaji <wahbeh.qardaji@gmail.com>',
    'Jimmy Retzlaff <jretz@yelp.com>',
    'Ned Rockson <ned@tellapart.com>',
    'Taro Sato <okomestudio@gmail.com>',
    'David Selassie <selassid@gmail.com>',
    'Paul Scott <paul@duedil.com>',
    'Alex Shkop <a.v.shkop@gmail.com>',
    'Jesse Shieh <jesse@adku.com>',
    'Isaac Slavitt <isaac.slavitt@gmail.com>',
    'Steve Spencer <steve@bigfrog.net>',
    'Jyry Suvilehto <jyry.suvilehto@iki.fi>',
    'Matthew Tai <mtai@adku.com>',
    'Paul Wais <pwais@yelp.com>',
    'Derek Wilson <jderekwilson@gmail.com>',
]
