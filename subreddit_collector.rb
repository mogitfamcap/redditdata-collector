class SubredditCollector
  def initialize(sql_client)
       @sql_client = sql_client
  end

  def collect(mode, subreddit_regex)
    if includes_wildcard? subreddit_regex then
      collect_all(mode, subreddit_regex)
    else
      collect_one_by_one(mode, subreddit_regex)
    end
  end

  def collect_one_by_one(mode, subreddit_regex)
    Util.log 'Collecting subreddit data: one by one'

    processed_count = 0

    subreddits_to_process = subreddit_regex.split '|'
    total_subreddit_count = subreddits_to_process.length

    subreddits_to_process.each do |subreddit_to_process|
      Util.log "Processing subreddit #{processed_count + 1}/#{total_subreddit_count}. Url: #{subreddit_to_process}"
      processed_count += 1

      res = RedditKit.subreddit subreddit_to_process.sub('/r/', '')
      @sql_client.add_subreddit(res, mode) unless res.nil?
      sleep 2
    end

    Util.log 'Collecting subreddits completed. Processes subreddits: ' + processed_count.to_s
  end

  def collect_all(mode, subreddit_regex)
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

      subreddits = safe_get_subreddits(options)

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

  def includes_wildcard?(subreddit_regex)
    subreddit_regex.include?('*') || subreddit_regex.include?('.') || subreddit_regex.include?('[') || subreddit_regex.include?('+')
  end

  def safe_get_subreddits(options)
    made_attempts = 0

    while (made_attempts < 3)
      begin
        made_attempts += 1
        subreddits = RedditKit.subreddits(options)
        if subreddits.nil?
          raise 'Subreddits are nil'
        end
        return subreddits
      rescue Exception => e
        Util.log e.message
        Util.log e.backtrace.inspect
        Util.log 'Sleeping for 10 seconds after error'
        sleep 10 unless made_attempts == 3
      end
    end

    raise 'Unable to get subreddits after 3 attempts'
  end
end
