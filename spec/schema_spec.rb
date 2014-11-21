require 'spec_helper'
require 'retries'
require 'redditkit'
require 'rspec'
require 'rspec/mocks'

require File.dirname(__FILE__) + '/../lib/redditdata-collector.rb'

describe Schema do
  before do
    Util.silent = true
    Util.nosleep = true
  end

  it 'Should find primary key names and values for subreddits' do
    expect(Schema.get_primary_key_name(Schema.subreddit_schema)).to eq('url')
    expect(Schema.get_primary_key_value(Schema.subreddit_schema, RedditKit::Subreddit.new({:data => {:url => '/r/funny'}}))).to eq('/r/funny')
  end

  it 'Should find primary key names and values for links' do
    expect(Schema.get_primary_key_name(Schema.link_schema)).to eq('permalink')
    expect(Schema.get_primary_key_value(Schema.link_schema, RedditKit::Link.new({:data => {:permalink => '/some_permalink'}}))).to eq('/some_permalink')
  end

  it 'Should find primary key names and values for users' do
    expect(Schema.get_primary_key_name(Schema.user_schema)).to eq('name')
    expect(Schema.get_primary_key_value(Schema.user_schema, RedditKit::User.new({:data => {:name => 'username'}}))).to eq('username')
  end
end
