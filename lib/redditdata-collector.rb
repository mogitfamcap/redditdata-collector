require 'sqlite3'
require 'redditkit'
require 'retries'

require File.dirname(__FILE__) + '/redditdata-collector/sql_client.rb'
require File.dirname(__FILE__) + '/redditdata-collector/subreddit_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/link_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/user_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/userlink_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/schema.rb'
require File.dirname(__FILE__) + '/redditdata-collector/util.rb'

module RedditdataCollector
  class << self
    def collect(path_to_database, dataset, mode, subreddit_regex)
      Util.log 'redditdata-collector-collector has started'

      sql_client = SqlClient::create(path_to_database, dataset, mode)
      redditkit = RedditKit

      case dataset
        when 'subreddits'
          subreddit_collector = SubredditCollector.new(sql_client, redditkit)
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
    end
  end
end
