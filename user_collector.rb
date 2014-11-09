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

      res = RedditKit.user(poster)

      if !res.nil? then
        @sql_client.add_user(res, mode)
      end
      sleep 2
    end

    Util.log 'Collecting users completed. Processes users: ' + processed_count.to_s
  end
end
