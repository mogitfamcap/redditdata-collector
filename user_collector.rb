class UserCollector
  def initialize(sql_client)
       @sql_client = sql_client
  end

  def collect(mode, subreddit_regex)
    Util.log 'Collecting user data'

    processed_count = 0

    posters = @sql_client.get_poster_names
    total_poster_count = posters.length

    posters.each do |poster|
      Util.log "Processing poster #{processed_count + 1}/#{total_poster_count}. Name: #{poster}"
      processed_count += 1

      if poster.include?('[') then
        next
      end

      res = nil
      handler = Proc.new do |exception, attempt_number, total_delay|
        Util.log exception.message
        Util.log "#{total_delay} seconds have passed"
      end
      with_retries(:max_tries => 5, :handler => handler, :base_sleep_seconds => 5, :max_sleep_seconds => 30, :rescue => [StandardError]) do |attempt|
        res = RedditKit.user(poster)
      end

      if !res.nil? then
        @sql_client.add_user(res, mode)
      end
      sleep 2
    end

    Util.log 'Collecting users completed. Processes users: ' + processed_count.to_s
  end
end
