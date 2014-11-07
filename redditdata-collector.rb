require 'sqlite3'
require 'redditkit'

require './sql_client.rb'
require './subreddit_collector.rb'

puts 'redditdata-collector has started'

if (ARGV.size != 3) then
  puts 'Usage: reddit-collector PATH_TO_DATABASE MODE SUBREDDIT_REGEX'
  exit 1
end

path_to_database = ARGV[0]
mode = ARGV[1]
subreddit_regex = ARGV[2]

sql_client = SqlClient::create(path_to_database, mode)
subreddit_collector = SubredditCollector.new sql_client

subreddit_collector.collect mode

puts 'redditdata-collector has finished'
