class LinkCollector
  def initialize(sql_client)
    @sql_client = sql_client
  end

  def collect(mode, subreddit_regex)

    Util.log 'Collecting link data'

    pattern = Regexp.new(subreddit_regex.downcase).freeze
    subreddit_urls = @sql_client.get_subreddits_urls

    selected_subreddit_urls = subreddit_urls.select { |subreddit_url| subreddit_url.downcase.match(pattern) }
    total_subreddit_count = selected_subreddit_urls.length
    processed_subreddit_count = 0

    selected_subreddit_urls.each do |subreddit_url|
      Util.log "Processing links in subreddit #{processed_subreddit_count + 1}/#{total_subreddit_count}. Url: #{subreddit_url}"
      collect_from_subreddit(mode, subreddit_url)
      processed_subreddit_count += 1
    end

    Util.log 'Collecting links completed'
  end

  def collect_from_subreddit(mode, subreddit_url)

    processed_count = 0
    last_link_id = nil

    options = { :category => :new, :limit => 100 }

    if mode == 'incremental' then
      latest_processed_fullname = @sql_client.get_last_link_in_subreddit_full_name(subreddit_url.sub('/r/', ''))
    end

    reached_processed = false

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
        links = RedditKit.links(subreddit_url.sub('/r/', ''), options)
      end

      if links.empty? then
        sleep(2)
        break
      end

      links.each do |link|
        fullname = link[:kind] + '_' + link[:id]
        last_link_id = fullname

        if fullname == latest_processed_fullname then
          reached_processed = true
          break
        end

        @sql_client.add_link(link, mode)

        processed_count += 1
      end

      if reached_processed then
        sleep(2)
        break
      end

      Util.log 'Collecting links in subreddit status: processed ' + processed_count.to_s + ' links'
      sleep(2)
    end


    Util.log 'Processing links in subreddit completed. Processed links: ' + processed_count.to_s
  end
end
