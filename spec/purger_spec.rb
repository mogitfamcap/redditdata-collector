require 'spec_helper'
require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe Purger do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should purge subreddits' do
    sql_client = SqlClientStub.new
    purger = Purger.new sql_client

    expect(sql_client).to receive(:delete_subreddit).with('/r/wtf')

    expect(sql_client).to receive(:delete_links).with('wtf')
    expect(sql_client).to receive(:delete_user).with('wtf_user_1')
    expect(sql_client).to receive(:delete_userlinks).with('wtf_user_1')

    expect(sql_client).to_not receive(:delete_links).with('funny')
    expect(sql_client).to_not receive(:delete_user).with('wtf_user_2')
    expect(sql_client).to_not receive(:delete_userlinks).with('wtf_user_2')

    purger.purge('/r/wtf')
  end

end

class SqlClientStub
  def get_subreddit_users(subreddit)
    if subreddit == 'wtf'
      return ['wtf_user_1', 'wtf_user_2']
    end
    fail "Unexpected subreddit: #{subreddit}"
  end

  def get_link_subreddits_by_user(user)
    case user
      when 'wtf_user_1'
        return ['wtf']
      when 'wtf_user_2'
        return ['wtf', 'funny']
      else
        fail "Unexpected user: #{user}"
    end
  end

  def method_missing(method_name, *args, &block)
  end
end
