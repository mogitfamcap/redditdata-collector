# TODO Consider turning into a module
class Schema
  TYPE_TEXT = 'TEXT'
  TYPE_INTEGER = 'INTEGER'
  TYPE_BOOLEAN = 'BOOLEAN'
  TYPE_REAL = 'REAL'


  def self.get_schema_for_dataset(dataset)
    case dataset
      when 'subreddits'
        return subreddit_schema
      when 'links'
        return link_schema
      when 'users'
        return user_schema
      when 'userlinks'
        return link_schema
      else
        raise ArgumentError, "Unknown dataset: #{dataset}"
    end
  end

  def self.subreddit_schema
    [
        { :name => 'url', :type => TYPE_TEXT, :primary_key? => true},
        { :name => 'id', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'display_name', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'submit_text', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'header_img', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'description_html', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'title', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'collapse_deleted_comments', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'over18', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'public_description_html', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'header_title', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'description', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'submit_link_label', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'accounts_active', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'public_traffic', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'header_size', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'subscribers', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'submit_text_label', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'user_is_moderator', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'name', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'user_is_contributor', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'public_description', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'comment_score_hide_mins', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'subreddit_type', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'submission_type', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'user_is_subscriber', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'kind', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'created', :type => TYPE_REAL, :primary_key? => false},
        { :name => 'created_utc', :type => TYPE_REAL, :primary_key? => false},
    ]
  end

  def self.link_schema
    [
        { :name => 'permalink', :type => TYPE_TEXT, :primary_key? => true},
        { :name => 'domain', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'banned_by', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'media_embed', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'subreddit', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'selftext_html', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'selftext', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'likes', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'user_reports', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'secure_media', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'link_flair_text', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'id', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'gilded', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'secure_media_embed', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'clicked', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'report_reasons', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'author', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'media', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'score', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'approved_by', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'over_18', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'hidden', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'thumbnail', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'subreddit_id', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'edited', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'link_flair_css_class', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'downs', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'mod_reports', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'saved', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'is_self', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'name', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'stickied', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'url', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'author_flair_text', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'title', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'ups', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'num_comments', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'distinguished', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'kind', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'created', :type => TYPE_REAL, :primary_key? => false},
        { :name => 'created_utc', :type => TYPE_REAL, :primary_key? => false},
    ]
  end

  def self.user_schema
    [
        { :name => 'name', :type => TYPE_TEXT, :primary_key? => true},
        { :name => 'is_friend', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'hide_from_robots', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'link_karma', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'comment_karma', :type => TYPE_INTEGER, :primary_key? => false},
        { :name => 'is_gold', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'is_mod', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'has_verified_email', :type => TYPE_BOOLEAN, :primary_key? => false},
        { :name => 'id', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'kind', :type => TYPE_TEXT, :primary_key? => false},
        { :name => 'created', :type => TYPE_REAL, :primary_key? => false},
        { :name => 'created_utc', :type => TYPE_REAL, :primary_key? => false},
    ]
  end

  def self.get_primary_key_name(schema)
    schema.each do |schema_element|
      if schema_element[:primary_key?] then
        return schema_element[:name]
      end
    end
  end

  def self.get_primary_key_value(schema, object)
    object.attributes[get_primary_key_name(schema).to_sym]
  end
end
