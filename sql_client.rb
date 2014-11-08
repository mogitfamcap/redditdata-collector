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

    commands_to_drop = @db.execute("select 'drop table ' || name || ';' from sqlite_master where type = 'table';")
    commands_to_drop.each do |command_to_drop|
      puts command_to_drop[0]
      @db.execute command_to_drop[0]
    end

    q = get_create_statement('subreddits', Schema.subreddit_schema)
    @db.execute(q)

    q = get_create_statement('links', Schema.link_schema)
    @db.execute(q)

    puts 'Bootstrapping competed'
  end

  def get_create_statement(name, schema)
    res = "CREATE TABLE #{name} ("
    schema.each do |schema_element|
      res += schema_element[:name] + ' ' + (schema_element[:type] != Schema::TYPE_BOOLEAN ? schema_element[:type] : Schema::TYPE_INTEGER)
      res += schema_element[:primary_key?] ? ' PRIMARY KEY' : ''
      res += ', '
    end
    res += 'updated_at REAL'
    res += ');'
    res
  end

  def get_insert_statement(name, schema)
    res = "INSERT INTO #{name} VALUES ("
    schema.each do |schema_element|
      res += '?, '
    end
    res += '?);'
    res
  end

  def get_update_statement(name, schema)
    res = "UPDATE #{name} SET "
    schema.each do |schema_element|
      res += schema_element[:name] + ' = ?, '
    end
    res += 'updated_at = ? '
    res += 'WHERE url = ?;'
    res
  end

  def get_insert_values(schema, object)
    result = []
    schema.each do |schema_element|
      case schema_element[:type]
        when Schema::TYPE_TEXT
          result.push object[schema_element[:name]].to_s
        when Schema::TYPE_INTEGER
          result.push object[schema_element[:name]]
        when Schema::TYPE_REAL
          result.push object[schema_element[:name]].to_i
        when Schema::TYPE_BOOLEAN
          result.push(object[schema_element[:name]] ? 1 : 0)
      end

    end
    result.push(Time.now.to_i)
    result
  end

  def get_update_values(schema, object)
    res = get_insert_values(schema, object)
    res.push(object[:url])
    res
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
    q = get_insert_statement('subreddits', Schema.subreddit_schema)
    v = get_insert_values(Schema.subreddit_schema, subreddit)
    @db.execute(q, v)
  end

  def insert_link(link)
    q = get_insert_statement('links', Schema.link_schema)
    v = get_insert_values(Schema.link_schema, link)
    @db.execute(q, v)
  end

  def update_subreddit(subreddit)
    q = get_update_statement('subreddits', Schema.subreddit_schema)
    v = get_update_values(Schema.subreddit_schema, subreddit)
    @db.execute(q, v)
  end

  def update_link(link)
    q = get_update_statement('links', Schema.link_schema)
    v = get_update_values(Schema.link_schema, link)
    @db.execute(q, v)
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
