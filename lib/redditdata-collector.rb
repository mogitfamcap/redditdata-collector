require 'sqlite3'
require 'redditkit'
require 'retries'

require File.dirname(__FILE__) + '/redditdata-collector/sql_client.rb'
require File.dirname(__FILE__) + '/redditdata-collector/subreddit_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/link_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/mode.rb'
require File.dirname(__FILE__) + '/redditdata-collector/user_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/userlink_collector.rb'
require File.dirname(__FILE__) + '/redditdata-collector/schema.rb'
require File.dirname(__FILE__) + '/redditdata-collector/util.rb'
require File.dirname(__FILE__) + '/redditdata-collector/dataset.rb'

module RedditdataCollector
  class << self
    def collect(path_to_database, dataset, mode, subreddit_regex)
      Util.log 'redditdata-collector has started'

      sql_client = SqlClient::create(path_to_database, dataset, mode)
      redditkit = RedditKit

      case dataset
        when Dataset::SUBREDDITS
          subreddit_collector = SubredditCollector.new(sql_client, redditkit)
          subreddit_collector.collect(mode, subreddit_regex)
        when Dataset::LINKS
          link_collector = LinkCollector.new(sql_client, redditkit)
          link_collector.collect(mode, subreddit_regex)
        when Dataset::USERS
          user_collector = UserCollector.new(sql_client, redditkit)
          user_collector.collect(mode, subreddit_regex)
        when Dataset::USERLINKS
          userlinks_collector = UserlinkCollector.new(sql_client, redditkit)
          userlinks_collector.collect(mode, subreddit_regex)
        else
          Util.log "dataset is not supported: #{dataset}"
      end

      Util.log 'redditdata-collector has finished'
    end

    def purge(path_to_database, subreddit_regex)
      Util.log 'redditdata-collector purge has started'

      Util.log 'redditdata-collector purge has finished'
    end
  end
end
