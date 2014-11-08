class SubredditCollector
  def initialize(sql_client)
       @sql_client = sql_client
  end

  def collect(mode, subreddit_regex)
    Util.log 'Collecting subreddit data'

    pattern = Regexp.new(subreddit_regex).freeze

    processed_count = 0
    matching_count = 0
    last_subreddit_id = nil

    options = { :category => :new, :limit => 100 }

    if mode == 'incremental' then
      latest_processed_fullname = @sql_client.get_last_subreddit_full_name
    end

    reached_processed = false

    while true do
      if !last_subreddit_id.nil? then
        options[:after] = last_subreddit_id
      end

      subreddits = RedditKit.subreddits(options)

      if subreddits.empty? then
        break
      end

      subreddits.each do |subreddit|
        fullname = subreddit[:kind] + '_' + subreddit[:id]
        last_subreddit_id = fullname

        if fullname == latest_processed_fullname then
          reached_processed = true
          break
        end

        if subreddit[:url].downcase.match(pattern) then
          @sql_client.add_subreddit(subreddit, mode)
          matching_count += 1
        end

        processed_count += 1
      end

      Util.log 'Collecting subreddits status: processed ' + processed_count.to_s + ' subreddits, ' + matching_count.to_s + ' matching'
      if reached_processed then
        break
      end

      sleep(2)
    end

    Util.log 'Collecting subreddits completed. Processes subreddits: ' + processed_count.to_s
  end
end
