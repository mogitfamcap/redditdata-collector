require './sql_client.rb'
require 'sqlite3'

puts 'redditdata-collector has started'

if (ARGV.size != 3) then
  puts 'Usage: reddit-collector PATH_TO_DATABASE MODE SUBREDDIT_REGEX'
  exit 1
end

path_to_database = ARGV[0]
mode = ARGV[1]
subreddit_regex = ARGV[2]

sql_client = SqlClient::create(path_to_database, mode)

puts 'redditdata-collector has finished'
