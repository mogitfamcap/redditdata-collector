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

    @db.execute(
        'CREATE TABLE links'\
        '('\
          'permalink TEXT PRIMARY KEY,'\
          'domain TEXT,'\
          'banned_by TEXT,'\
          'media_embed TEXT,'\
          'subreddit TEXT,'\
          'selftext_html TEXT,'\
          'selftext TEXT,'\
          'likes TEXT,'\
          'user_reports TEXT,'\
          'secure_media TEXT,'\
          'link_flair_text TEXT,'\
          'id TEXT,'\
          'gilded INTEGER,'\
          'secure_media_embed TEXT,'\
          'clicked INTEGER,'\
          'report_reasons TEXT,'\
          'author INTEGER,'\
          'media TEXT,'\
          'score INTEGER,'\
          'approved_by TEXT,'\
          'over_18 INTEGER,'\
          'hidden INTEGER,'\
          'thumbnail TEXT,'\
          'subreddit_id TEXT,'\
          'edited INTEGER,'\
          'link_flair_css_class TEXT,'\
          'downs INTEGER,'\
          'mod_reports TEXT,'\
          'saved INTEGER,'\
          'is_self INTEGER,'\
          'name TEXT,'\
          'stickied INTEGER,'\
          'url TEXT,'\
          'author_flair_text TEXT,'\
          'text TEXT,'\
          'ups INTEGER,'\
          'num_comments INTEGER,'\
          'visited INTEGER,'\
          'num_reports TEXT,'\
          'distinguished   TEXT,'\
          'kind INTEGER,'\
          'created_at REAL,'\
          'updated_at REAL'\
        ');'
    )

    puts 'Bootstrapping competed'
  end

  def add_subreddit(subreddit, mode)
    #if mode == 'bootstrap' || mode == 'incremental'
    #    insert_subreddit subreddit
    #  return
    #end

    #if mode == 'full'
      if !subreddit_exists?(subreddit) then
        insert_subreddit subreddit
      else
        update_subreddit subreddit
      end
      return
    #end
  end

  def add_link(link, mode)
    #if mode == 'bootstrap' || mode == 'incremental'
    #  insert_link link
    #  return
    #end

    #if mode == 'full'
      if !link_exists?(link) then
        insert_link link
      else
        update_link link
      end
      return
    #end
  end

  def subreddit_exists?(subreddit)
    q = 'SELECT url FROM subreddits WHERE url = ?;'
    res = @db.execute(q, subreddit[:url])
    !res.empty?
  end

  def link_exists?(link)
    q = 'SELECT permalink FROM links WHERE permalink = ?;'
    res = @db.execute(q, link[:permalink])
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

  def insert_link(link)
    q = 'INSERT INTO links VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'
    @db.execute(
        q,
          link[:permalink],
          link[:domain],
          link[:banned_by],
          link[:media_embed].to_s,
          link[:subreddit],
          link[:selftext_html],
          link[:selftext],
          link[:likes],
          link[:user_reports].to_s,
          link[:secure_media],
          link[:link_flair_text],
          link[:id],
          link[:gilded],
          link[:secure_media_embed],
          link[:clicked] ? 1 : 0,
          link[:report_reasons],
          link[:author],
          link[:media].to_s,
          link[:score],
          link[:approved_by],
          link[:over_18] ? 1 : 0,
          link[:hidden] ? 1 : 0,
          link[:thumbnail],
          link[:subreddit_id],
          link[:edited] ? 1 : 0,
          link[:link_flair_css_class],
          link[:downs],
          link[:mod_reports].to_s,
          link[:saved] ? 1 : 0,
          link[:is_self] ? 1 : 0,
          link[:name],
          link[:stickied] ? 1 : 0,
          link[:url],
          link[:author_flair_text],
          link[:text],
          link[:ups],
          link[:num_comments],
          link[:visited] ? 1 : 0,
          link[:num_reports],
          link[:distinguished  ],
          link[:kind],
          link[:created_at].to_i,
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

  def update_link(link)
    q = 'UPDATE links SET '\
          'permalink=?, '\
          'domain=?, '\
          'banned_by=?, '\
          'media_embed=?, '\
          'subreddit=?, '\
          'selftext_html=?, '\
          'selftext=?, '\
          'likes=?, '\
          'user_reports=?, '\
          'secure_media=?, '\
          'link_flair_text=?, '\
          'id=?, '\
          'gilded=?, '\
          'secure_media_embed=?, '\
          'clicked=?, '\
          'report_reasons=?, '\
          'author=?, '\
          'media=?, '\
          'score=?, '\
          'approved_by=?, '\
          'over_18=?, '\
          'hidden=?, '\
          'thumbnail=?, '\
          'subreddit_id=?, '\
          'edited=?, '\
          'link_flair_css_class=?, '\
          'downs=?, '\
          'mod_reports=?, '\
          'saved=?, '\
          'is_self=?, '\
          'name=?, '\
          'stickied=?, '\
          'url=?, '\
          'author_flair_text=?, '\
          'text=?, '\
          'ups=?, '\
          'num_comments=?, '\
          'visited=?, '\
          'num_reports=?, '\
          'distinguished  =?, '\
          'kind=?, '\
          'created_at=?, '\
          'updated_at=?'\
        'WHERE permalink=?;'

    @db.execute(
        q,
        link[:permalink],
        link[:domain],
        link[:banned_by],
        link[:media_embed].to_s,
        link[:subreddit],
        link[:selftext_html],
        link[:selftext],
        link[:likes],
        link[:user_reports].to_s,
        link[:secure_media],
        link[:link_flair_text],
        link[:id],
        link[:gilded],
        link[:secure_media_embed],
        link[:clicked] ? 1 : 0,
        link[:report_reasons],
        link[:author],
        link[:media].to_s,
        link[:score],
        link[:approved_by],
        link[:over_18] ? 1 : 0,
        link[:hidden] ? 1 : 0,
        link[:thumbnail],
        link[:subreddit_id],
        link[:edited] ? 1 : 0,
        link[:link_flair_css_class],
        link[:downs],
        link[:mod_reports].to_s,
        link[:saved] ? 1 : 0,
        link[:is_self] ? 1 : 0,
        link[:name],
        link[:stickied] ? 1 : 0,
        link[:url],
        link[:author_flair_text],
        link[:text],
        link[:ups],
        link[:num_comments],
        link[:visited] ? 1 : 0,
        link[:num_reports],
        link[:distinguished  ],
        link[:kind],
        link[:created_at].to_i,
        Time.now.to_i,
        link[:permalink]
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

  def get_last_link_in_subreddit_full_name(subreddit)
    q = 'SELECT (kind || "_" || id) AS fullname FROM links WHERE subreddit=? ORDER BY created_at DESC LIMIT 1;'
    q_res = @db.execute(q, subreddit)

    if q_res.nil? then
      return nil
    end

    q_res[0][0]
  end

  def get_subreddits_urls
    q = 'SELECT url FROM subreddits;'
    q_res = @db.execute(q)

    q_res.map { |row| row[0] }
  end
end
