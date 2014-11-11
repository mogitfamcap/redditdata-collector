require 'sqlite3'
require 'redditkit'
require 'retries'

require File.dirname(__FILE__) + '/sql_client.rb'
require File.dirname(__FILE__) + '/subreddit_collector.rb'
require File.dirname(__FILE__) + '/link_collector.rb'
require File.dirname(__FILE__) + '/user_collector.rb'
require File.dirname(__FILE__) + '/userlink_collector.rb'
require File.dirname(__FILE__) + '/schema.rb'
require File.dirname(__FILE__) + '/util.rb'

if (ARGV.size != 4) then
  puts 'Usage: reddit-collector PATH_TO_DATABASE DATASET MODE SUBREDDIT_REGEX'
  exit 1
end

Util.log 'redditdata-collector has started'

path_to_database = ARGV[0]
dataset = ARGV[1]
mode = ARGV[2]
subreddit_regex = ARGV[3]

sql_client = SqlClient::create(path_to_database, dataset, mode)

case dataset
  when 'subreddits'
    subreddit_collector = SubredditCollector.new sql_client
    subreddit_collector.collect(mode, subreddit_regex)
  when 'links'
    link_collector = LinkCollector.new sql_client
    link_collector.collect(mode, subreddit_regex)
  when 'users'
    user_collector = UserCollector.new sql_client
    user_collector.collect(mode, subreddit_regex)
  when 'userlinks'
    userlinks_collector = UserlinkCollector.new sql_client
    userlinks_collector.collect(mode, subreddit_regex)
  else
    Util.log "dataset is not supported: #{dataset}"
end

Util.log 'redditdata-collector has finished'
