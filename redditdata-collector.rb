require 'sqlite3'
require 'redditkit'

require './sql_client.rb'
require './subreddit_collector.rb'
require './link_collector.rb'
require './schema.rb'
require './util.rb'

if (ARGV.size != 4) then
  puts 'Usage: reddit-collector PATH_TO_DATABASE DATASET MODE SUBREDDIT_REGEX'
  exit 1
end

Util.log 'redditdata-collector has started'

path_to_database = ARGV[0]
dataset = ARGV[1]
mode = ARGV[2]
subreddit_regex = ARGV[3]

sql_client = SqlClient::create(path_to_database, mode)

case dataset
  when 'subreddits'
    subreddit_collector = SubredditCollector.new sql_client
    subreddit_collector.collect mode
  when 'links'
    link_collector = LinkCollector.new sql_client
    link_collector.collect(mode, subreddit_regex)
  else
    Util.log "dataset is not supported: #{dataset}"
end

Util.log 'redditdata-collector has finished'
