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
          'url TEXT PRIMARY KEY,'\
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
          'user_is_contributor INTEGER,'\
          'public_description TEXT,'\
          'comment_score_hide_mins INTEGER,'\
          'subreddit_type INTEGER,'\
          'submission_type TEXT,'\
          'user_is_subscriber INTEGER,'\
          'kind INTEGER,'\
          'created_at REAL,'\
          'updated_at REAL'\
        ');'
    )

    puts 'Bootstrapping competed'
  end

  def add_subreddit(subreddit, mode)
    if mode == 'bootstrap' || mode == 'incremental'
        insert_subreddit subreddit
      return
    end

    if mode == 'full'
      if !subreddit_exists?(subreddit) then
        insert_subreddit subreddit
      else
        update_subreddit subreddit
      end
      return
    end
  end

  def subreddit_exists?(subreddit)
    q = 'SELECT url FROM subreddits WHERE url = ?;'
    res = @db.execute(q, subreddit[:url])
    !res.empty?
  end

  def insert_subreddit(subreddit)
    q = 'INSERT INTO subreddits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
    @db.execute(
        q,
          subreddit[:url],
          subreddit[:id],
          subreddit[:display_name],
          subreddit[:submit_text],
          subreddit[:header_img],
          subreddit[:description_html],
          subreddit[:title],
          subreddit[:collapse_deleted_comments],
          subreddit[:over18] ? 1 : 0,
          subreddit[:public_description_html],
          subreddit[:header_title],
          subreddit[:description],
          subreddit[:submit_link_label],
          subreddit[:accounts_active],
          subreddit[:public_traffic] ? 1 : 0,
          subreddit[:header_size].to_s,
          subreddit[:subscribers],
          subreddit[:submit_text_label],
          subreddit[:user_is_moderator],
          subreddit[:name],
          subreddit[:user_is_contributor],
          subreddit[:public_description],
          subreddit[:comment_score_hide_mins],
          subreddit[:subreddit_type],
          subreddit[:submission_type],
          subreddit[:user_is_subscriber],
          subreddit[:kind],
          subreddit[:created_at].to_i,
          Time.now.to_i
    )
  end

  def update_subreddit(subreddit)
    q = 'UPDATE subreddits SET '\
          'url=?, '\
          'id=?, '\
          'display_name=?, '\
          'submit_text=?, '\
          'header_img=?, '\
          'description_html=?, '\
          'title=?, '\
          'collapse_deleted_comments=?, '\
          'over18=?, '\
          'public_description_html=?, '\
          'header_title=?, '\
          'description=?, '\
          'submit_link_label=?, '\
          'accounts_active=?, '\
          'public_traffic=?, '\
          'header_size=?, '\
          'subscribers=?, '\
          'submit_text_label=?, '\
          'user_is_moderator=?, '\
          'name=?, '\
          'user_is_contributor=?, '\
          'public_description=?, '\
          'comment_score_hide_mins=?, '\
          'subreddit_type=?, '\
          'submission_type=?, '\
          'user_is_subscriber=?, '\
          'kind=?, '\
          'created_at=?, '\
          'updated_at=? '\
        'WHERE url=?;'

    @db.execute(
        q,
        subreddit[:url],
        subreddit[:id],
        subreddit[:display_name],
        subreddit[:submit_text],
        subreddit[:header_img],
        subreddit[:description_html],
        subreddit[:title],
        subreddit[:collapse_deleted_comments],
        subreddit[:over18] ? 1 : 0,
        subreddit[:public_description_html],
        subreddit[:header_title],
        subreddit[:description],
        subreddit[:submit_link_label],
        subreddit[:accounts_active],
        subreddit[:public_traffic] ? 1 : 0,
        subreddit[:header_size].to_s,
        subreddit[:subscribers],
        subreddit[:submit_text_label],
        subreddit[:user_is_moderator],
        subreddit[:name],
        subreddit[:user_is_contributor],
        subreddit[:public_description],
        subreddit[:comment_score_hide_mins],
        subreddit[:subreddit_type],
        subreddit[:submission_type],
        subreddit[:user_is_subscriber],
        subreddit[:kind],
        subreddit[:created_at].to_i,
        Time.now.to_i,
        subreddit[:url]
    )
  end

  def get_last_subreddit_full_name
    q = 'SELECT (kind || "_" || id) AS fullname FROM subreddits ORDER BY created_at DESC LIMIT 1;'
    q_res = @db.execute(q)

    if q_res.nil? then
      return nil
    end

    q_res[0][0]
  end
end
