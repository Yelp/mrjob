#!/usr/bin/env ruby

exit unless __FILE__ == $0

require 'optparse'
require 'json'

JSON_STEPS = JSON[[
 {type: 'streaming', mapper: {type: 'script', pre_filter: 'cat'},
                     reducer: {type: 'script', pre_filter: 'cat'}},
]]

def write(key, value)
  print "#{key}\t#{value}\n"
end

EACH_WORD_RE = /\w+/
def lines_to_word_occurrences
  STDIN.each_line do |line|
    line.scan(EACH_WORD_RE).each {|word| write(word, 1)}
  end
end

def sum_word_occurrences
  STDIN.each_line do |line|
    if @last_word == (word = line.split.first)
      @count += 1
    else
      write(@last_word, @count)
      @count = 1
    end
    @last_word = word
  end
end

options = {}
OptionParser.new do |opts|
  opts.banner = <<-EOS
    Local usage:
    $ cat w.txt | mrjob/examples/mr_wc.rb --step-num=0 --mapper | sort |
                  mrjob/examples/mr_wc.rb --step-num=0 --reducer | sort -nk 2"
  EOS

  opts.separator ""
  opts.separator "Specific options:"

  opts.on("--steps", "Print the steps and exit") {options[:steps] = true}
  opts.on("--mapper", "Run a mapper") {|v|options[:mapper] = true}
  opts.on("--reducer", "Run a reducer") {|v|options[:reducer] = true}
  opts.on("--step-num N", Integer, "Change number of steps") {|v|options[:step_num] = v}
  opts.on("--reducer", "Run a reducer") {|v|options[:reducer] = true}
end.parse!

# Requirements of this MrJob
if options[:steps]
  puts JSON_STEPS
elsif !options[:step_num]
  fail 'If --steps isn\'t present, --step-num=N is'
elsif (!options[:mapper] && !options[:reducer]) ||
      (options[:mapper] && options[:reducer])
  fail 'If --steps isn\'t present, you must specify exactly one --mapper or --reducer'
elsif options[:mapper]
  case step_num = options[:step_num]
    when 0
      lines_to_word_occurrences
    else
      fail "--step-num #{step_num}"
  end
elsif options[:reducer]
  case step_num = options[:step_num]
    when 0
      sum_word_occurrences
    else
      fail "--step-num #{step_num}"
  end
end
