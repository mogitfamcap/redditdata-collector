class UserlinkCollector
  def initialize(sql_client)
    @sql_client = sql_client
  end

  def collect(mode, subreddit_regex)

    Util.log 'Collecting user link data'

    users = @sql_client.get_all_user_names

    total_user_count = users.length
    processed_user_count = 0

    users.each do |user|
      Util.log "Processing links of user #{processed_user_count + 1}/#{total_user_count}. Name: #{user}"
      collect_for_user(mode, user)
      processed_user_count += 1
    end

    Util.log 'Collecting user links completed'
  end

  def collect_for_user(mode, user)
    processed_count = 0
    last_link_id = nil

    options = { :category => :submitted, :limit => 100 }

    if mode == 'incremental' then
      #latest_processed_fullname = @sql_client.get_last_link_in_subreddit_full_name(subreddit_url.sub('/r/', ''))
    end

    while true do
      if !last_link_id.nil? then
        options[:after] = last_link_id
      end

      links = nil
      handler = Proc.new do |exception, attempt_number, total_delay|
        Util.log exception.message
        Util.log "#{total_delay} seconds have passed"
      end
      with_retries(:max_tries => 5, :handler => handler, :base_sleep_seconds => 5, :max_sleep_seconds => 30, :rescue => [StandardError]) do |attempt|
        links = RedditKit.user_content(user, options)
      end

      if links.nil? then
        Util.log 'WARNING: links is nil'
        sleep(2)
        break
      end

      found_link = false
      links.each do |link|
        fullname = link[:kind] + '_' + link[:id]
        last_link_id = fullname

        if !link.is_a? RedditKit::Link then
          next
        end
        found_link = true

        @sql_client.add_userlink(link, mode)

        processed_count += 1
      end

      if !found_link then
        sleep(2)
        break
      end

      Util.log 'Collecting links of user status: processed ' + processed_count.to_s + ' links'
      sleep(2)
    end


    Util.log 'Processing links of user completed. Processed links: ' + processed_count.to_s
  end
end
