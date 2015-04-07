# -*- coding: utf-8 -*-

# Copyright 2009-2015 Yelp and Contributors
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

__author__ = 'David Marin <dm@davidmarin.org>'

__credits__ = [
    'Marc Abramowitz <msabramo@gmail.com>',
    'Jordan Andersen <jordandandersen@gmail.com>',
    'Tom Arnfeld <tarnfeld@me.com>',
    'Martin Baeuml <baeuml@kit.edu>',
    'Hunter Blanks <hblanks@monetate.com>',
    'Jim Blomo <jblomo@yelp.com>',
    'Reno Bowen <renobowen@gmail.com>',
    'James Brown <jbrown@uber.com>',
    'Kevin Burke <kevin@twilio.com>',
    'Ewen Cheslack-Postava <me@ewencp.org>',
    'Ben Chess <bchess@yelp.com>',
    'Jordan Christensen <jc@kobo.com>',
    'Jonathan Chu <milki@yelp.com>',
    'David Dehghan <ddehghan@gmail.com>',
    'Adam Derewecki <derewecki@gmail.com>',
    'Nick Dimiduk <ndimiduk@gmail.com>',
    'Tom Dooner <tomdooner@gmail.com>',
    'Tomer Elmalem <telmalem@gmail.com>',
    'Sudarshan Gaikaiwari <sudarshan@acm.org>',
    'Brett Gibson <brett@swiftserve.com>',
    'Benjamin Goldenberg <benjamin@yelp.com',
    'Buck Golemon <buck@yelp.com>',
    'Peter Harrington <peter.b.harrington@gmail.com>',
    'Brandon Haynes <bhaynes@fas.harvard.edu>',
    'Tim Henderson <tim.tadh@gmail.com>',
    'Tom Hennigan <tomhennigan@gmail.com>',
    'Brett Hoerner <brett@bretthoerner.com>',
    'Ya-Lin Huang <yalinh@yelp.com>',
    'Evan Klitzke <evan@eklitzke.org>',
    'Tom Janofsky <tjanofsky@monetate.com>',
    'Stephen Johnson <steve@steveasleep.com>',
    'Ben Jolitz <Ben.Jolitz.Acxiom.com>',
    'Matt Jones <matt@mhjones.org>',
    'Kamil Kisiel <kamil@kamilkisiel.net>',
    'Evan Klitzke <evan@eklitzke.org>',
    'Alex Konradi <alexkonradi@gmail.com>',
    'Nikolaos Koutsopoulos <nhk@mochimedia.com>',
    'Julian Krause <juliank@yelp.com>',
    'Pai-Wei Lai <paiwei@yelp.com>',
    'Boris Lau <boris.w.lau@gmail.com>',
    'Andrew Lenards <andrew.lenards@gmail.com>',
    'Tianhui Michael Li <tianhuil@cs.princeton.edu>',
    'Shusen Liu <liushusen.smart@gmail.com>',
    'Robert Leftwich <rl.0x0@eml.cc>',
    'Adrian Maceiras <amac425@utexas.edu>',
    'Baris Metin <bmetin@yelp.com>',
    'Konark Modi <modi.konark@gmail.com>',
    'Tetsuya Morimoto <tetsuya.morimoto@gmail.com>',
    'Hendrik Muhs <hendrik@cliqz.com>',
    'Zach Musgrave <zmusgrave@gmail.com>',
    'Sean Myers <seanmyers0608@gmail.com>',
    'Spencer Nelson <s@spenczar.com>',
    'Dávid Nemeskey <david@cliqz.com>',
    'Daniel Nephin <dnephin@yelp.com>',
    'Oliver Nicholas <bigo@wonlove.net>',
    'Santeri Paavolainen <santtu@iki.fi>',
    'Matt Perry <matt@unshift.net>',
    'Kien Pham <kien@sendgrid.com>',
    'Andrew Price <andrew.price@ensighten.com>',
    'Wahbeh Qardaji <wahbeh.qardaji@gmail.com>',
    'Anusha Rajan <anusha@yelp.com>',
    'Pavel Repin <prepin@gmail.com>',
    'Jimmy Retzlaff <jretz@yelp.com>',
    'Ned Rockson <ned@tellapart.com>',
    'Alain Rodriguez <eagle5command@gmail.com>',
    'Pedro Emanuel de Castro Faria Salgado <steenzout@ymail.com>',
    'Dan Frank <danielhfrank@gmail.com>',
    'Taro Sato <okomestudio@gmail.com>',
    'Paul Scott <paul@duedil.com>',
    'David Selassie <selassid@gmail.com>',
    'Alex Shkop <a.v.shkop@gmail.com>',
    'Jesse Shieh <jesse@adku.com>',
    'Isaac Slavitt <isaac.slavitt@gmail.com>',
    'Steve Spencer <steve@bigfrog.net>',
    'Jyry Suvilehto <jyry.suvilehto@iki.fi>',
    'Phil Swanson <swanson.p@gmail.com>',
    'Matthew Tai <mtai@adku.com>',
    'Diogo Terror <me@diogoterror.com>',
    'Paul Wais <pwais@yelp.com>',
    'Derek Wilson <jderekwilson@gmail.com>',
    'Tao Yu <taoyu@yelp.com>',
    'Andrea Zonca <andrea.zonca@gmail.com>',
]

__version__ = '0.4.3'
