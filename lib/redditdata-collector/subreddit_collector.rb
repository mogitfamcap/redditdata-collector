class SubredditCollector
  def initialize(sql_client, redditkit)
    @sql_client = sql_client
    @redditkit = redditkit
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

      res = nil
      handler = Proc.new do |exception, attempt_number, total_delay|
        Util.log exception.message
        Util.log "#{total_delay} seconds have passed"
      end
      with_retries(:max_tries => 5, :handler => handler, :base_sleep_seconds => 5, :max_sleep_seconds => 30, :rescue => [StandardError]) do |attempt|
        begin
          res = @redditkit.subreddit subreddit_to_process.sub('/r/', '')
        rescue RedditKit::PermissionDenied
          Util.log 'PermissionDenied: skipping subreddit'
          Util.sleep 2
          next
        end
      end

      @sql_client.add_subreddit(res, mode) unless res.nil?
      Util.sleep 2
    end

    Util.log 'Collecting subreddits completed. Processes subreddits: ' + processed_count.to_s
  end

  def collect_all(mode, subreddit_regex)
    # TODO implement this method
    raise StandardError.new('SubredditCollector.collect_all is not supported')
  end

  def includes_wildcard?(subreddit_regex)
    subreddit_regex.include?('*') || subreddit_regex.include?('.') || subreddit_regex.include?('[') || subreddit_regex.include?('+')
  end
end
