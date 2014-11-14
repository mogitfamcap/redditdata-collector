class UserlinkCollector
  def initialize(sql_client, redditkit)
    @sql_client = sql_client
    @redditkit = redditkit
  end

  def collect(mode, subreddit_regex)

    Util.log 'Collecting user link data'
    users = get_users_to_process(mode)

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

    while true do
      if !last_link_id.nil? then
        options[:after] = last_link_id
      end

      links = nil

      begin
        handler = Proc.new do |exception, attempt_number, total_delay|
          Util.log exception.message
          Util.log "#{total_delay} seconds have passed"
        end
        with_retries(:max_tries => 5, :handler => handler, :base_sleep_seconds => 10, :max_sleep_seconds => 100, :rescue => [StandardError]) do |attempt|
          links = @redditkit.user_content(user, options)
        end
      rescue StandardError
        Util.log "Failed retrieving userlinks for user #{user}"
      end

      if links.nil? then
        Util.log 'WARNING: links is nil'
        Util.sleep(2)
        break
      end

      if links.empty? then
        Util.sleep(2)
        break
      end

      found_link = false
      userlinks_to_add = []
      links.each do |link|
        fullname = link[:kind] + '_' + link[:id]
        last_link_id = fullname

        if !link.is_a? RedditKit::Link then
          next
        end
        found_link = true

        userlinks_to_add.push link

        processed_count += 1
      end

      if !found_link then
        Util.sleep(2)
        break
      end

      @sql_client.bulk_add_items('userlinks', userlinks_to_add)

      Util.log 'Collecting links of user status: processed ' + processed_count.to_s + ' links'
      Util.sleep(2)
    end


    Util.log 'Processing links of user completed. Processed links: ' + processed_count.to_s
  end

  private

  def get_users_to_process(mode)
    case mode
      when Mode::INCREMENTAL
        @sql_client.get_all_user_names - @sql_client.get_all_users_with_userlinks
      else
        @sql_client.get_all_user_names
    end
  end
end
