class Purger
  def initialize(sql_client)
    @sql_client = sql_client
  end

  # TODO purge multiple subreddits at once
  def purge(subreddit_url)
    all_subreddit_users = @sql_client.get_subreddit_users(Subreddit::url_to_name(subreddit_url))
    purged_user_count = 0

    all_subreddit_users.each do |user|
      if purge_user_needed?(user, subreddit_url) then
        Util.log "Purging user #{user} is: needed"
        purge_user(user, subreddit_url)
        purged_user_count += 1
      else
        Util.log "Purging user #{user} is: not needed"
      end
    end

    Util.log 'Deleting links'
    @sql_client.delete_links(Subreddit::url_to_name(subreddit_url))
    Util.log 'Deleting subreddit'
    @sql_client.delete_subreddit(subreddit_url)

    Util.log "Purged #{purged_user_count} users"
  end

  private

  def purge_user_needed?(user, subreddit_url)
    link_subreddits_by_user = @sql_client.get_link_subreddits_by_user(user)
    !link_subreddits_by_user.any? { |link_subreddit| link_subreddit != Subreddit::url_to_name(subreddit_url)}
  end

  def purge_user(user, subreddit_url)
    @sql_client.delete_user(user)
    @sql_client.delete_userlinks(user)
  end
end
