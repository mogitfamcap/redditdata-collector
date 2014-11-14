require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe UserlinkCollector do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should collect links' do
    sql_client = double(SqlClient)
    redditkit = RedditKitMockForUserlinks.new

    userlink_collector = UserlinkCollector.new(sql_client, redditkit)
    allow(sql_client).to receive(:get_all_user_names).and_return(['username_1', 'username_2'])

    expect(sql_client).to receive(:bulk_add_userlinks).exactly(2).times

    userlink_collector.collect('full', '/r/funny|/r/wtf')
  end
end

class RedditKitMockForUsers
  def user(user_name)
    case user_name
      when 'username_1'
        return RedditKit::User.new({:data => {:name => 'username_1'}})
      when 'username_2'
        return RedditKit::User.new({:data => {:name => 'username_2'}})
      else
        return nil
    end
  end
end

class RedditKitMockForUserlinks
  def user_content(username, options)
    case username
      when 'username_1'
        result = [ RedditKit::Link.new( {:data => {:permalink => '/funny_link_1', :kind => 'link_kind', :id => 'link_funny_id_1'}, :kind => 'link_kind', :id => 'link_funny_id_1'}) ]
      when 'username_2'
        result = [ RedditKit::Link.new( {:data => {:permalink => '/wtf_link_1', :kind => 'link_kind', :id => 'link_wtf_id_1'}, :kind => 'link_kind', :id => 'link_wtf_id_1'}) ]
      else
        return nil
    end
    result.select { |subreddit|
      !options.has_key?(:after) || options[:after].nil? || 'link_kind' + '_' + subreddit.attributes[:id] < options[:after]
    }
  end
end
