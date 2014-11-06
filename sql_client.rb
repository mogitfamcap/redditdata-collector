class SqlClient
  def self.create(path_to_database, mode)
    db = SQLite3::Database.new path_to_database
    client = SqlClient.new(db, mode)
    if mode == 'bootstrap'
      client.bootstrap
    end
    client
  end

  def initialize(db, mode)
    @db = db
    @mode = mode
  end

  def bootstrap
    puts 'Bootstrapping database'

    @db.execute(
        'CREATE TABLE subreddits'\
        '('\
          'url TEXT,'\
          'id TEXT,'\
          'display_name TEXT,'\
          'submit_text TEXT,'\
          'header_img TEXT,'\
          'description_html TEXT,'\
          'title TEXT,'\
          'collapse_deleted_comments INTEGER,'\
          'over18 INTEGER,'\
          'public_description_html TEXT,'\
          'header_title TEXT,'\
          'description TEXT,'\
          'submit_link_label TEXT,'\
          'accounts_active TEXT,'\
          'public_traffic TEXT,'\
          'header_size INTEGER,'\
          'subscribers INTEGER,'\
          'submit_text_label TEXT,'\
          'user_is_moderator INTEGER,'\
          'name TEXT,'\
          'created REAL,'\
          'user_is_contributor INTEGER,'\
          'public_description TEXT,'\
          'comment_score_hide_mins INTEGER,'\
          'subreddit_type INTEGER,'\
          'submission_type TEXT,'\
          'user_is_subscriber INTEGER'\
        ');'
    )

    puts 'Bootstrapping competed'
  end
end
