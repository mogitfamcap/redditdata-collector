require 'spec_helper'
require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe SubredditCollector do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should collect subreddits' do
    sql_client = double()
    redditkit = RedditKitMockForSubreddits.new

    subreddit_collector = SubredditCollector.new(sql_client, redditkit)

    expect(sql_client).to receive(:add_item).with('subreddits', redditkit.subreddit('funny'))
    expect(sql_client).to receive(:add_item).with('subreddits', redditkit.subreddit('wtf'))

    subreddit_collector.collect('full', '/r/funny|/r/wtf')
  end

  it 'Should raise error because wildcards aren\'t supported' do
    sql_client = double()
    redditkit = RedditKitMockForSubreddits.new

    subreddit_collector = SubredditCollector.new(sql_client, redditkit)

    expect { subreddit_collector.collect('full', '/r/funny*') }.to raise_error(StandardError)
  end
end

class RedditKitMockForSubreddits
  def subreddit(subreddit_name)
    case subreddit_name
      when 'funny'
        return RedditKit::Subreddit.new({:data => {:url => '/r/funny'}})
      when 'wtf'
        return RedditKit::Subreddit.new({:data => {:url => '/r/wtf'}})
      else
        return nil
    end
  end
end