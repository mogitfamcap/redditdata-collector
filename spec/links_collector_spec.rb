require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe LinkCollector do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should collect links' do
    sql_client = double(SqlClient)
    allow(sql_client).to receive(:get_subreddits_urls).and_return(['/r/funny', '/r/wtf', '/r/AskReddit'])

    redditkit = RedditKitMockForLinks.new

    links_collector = LinkCollector.new(sql_client, redditkit)

    expect(sql_client).to receive(:bulk_add_items).exactly(2).times

    links_collector.collect('full', '/r/funny|/r/wtf')
  end
end

class RedditKitMockForLinks
  def links(subreddit_name, options)
    case subreddit_name
      when 'funny'
        result = [ RedditKit::Link.new( {:data => {:permalink => '/funny_link_1', :kind => 'link_kind', :id => 'link_funny_id_1'}, :kind => 'link_kind', :id => 'link_funny_id_1'}) ]
      when 'wtf'
        result = [ RedditKit::Link.new( {:data => {:permalink => '/wtf_link_1', :kind => 'link_kind', :id => 'link_wtf_id_1'}, :kind => 'link_kind', :id => 'link_wtf_id_1'}) ]
      when '/r/AskReddit'
        result = nil
      else
        result = nil
    end
    result.select { |subreddit|
      !options.has_key?(:after) || options[:after].nil? || 'link_kind' + '_' + subreddit.attributes[:id] < options[:after]
    }
  end
end
