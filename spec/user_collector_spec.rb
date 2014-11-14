require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe UserCollector do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should collect users' do
    sql_client = double()
    redditkit = RedditKitMockForUsers.new
    user_collector = UserCollector.new(sql_client, redditkit)


    allow(sql_client).to receive(:get_poster_names).and_return(['username_1', 'username_2'])
    expect(sql_client).to receive(:add_user).with(redditkit.user('username_1'), 'full')
    expect(sql_client).to receive(:add_user).with(redditkit.user('username_2'), 'full')

    user_collector.collect('full', '/r/funny|/r/wtf')
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