class SubredditCollector
  def initialize(sql_client)
       @sql_client = sql_client
  end

  def collect(mode)
    puts 'Collecting subreddit data'

    processed_count = 0
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

        @sql_client.add_subreddit(subreddit, mode)

        processed_count += 1
      end

      if reached_processed then
        break
      end

      sleep(2)
    end

    puts 'Collecting subreddits completed. Processes subreddits: ' + processed_count.to_s
  end
end
