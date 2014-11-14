class SqlClient
  def self.create(path_to_database, dataset, mode)
    db = SQLite3::Database.new path_to_database
    client = SqlClient.new(db, mode)

    client.setup_tables(dataset, mode)

    client
  end

  def initialize(db, mode)
    @db = db
    @mode = mode
  end

  def setup_tables(dataset, mode)
    Util.log 'Setting up tables'

    if mode == 'bootstrap'
      Util.log "Dropping table #{dataset}"
      @db.execute "DROP TABLE IF EXISTS #{dataset} ;"
    end

    Util.log "Creating table #{dataset}"
    q = get_create_statement(dataset, Schema.get_schema_for_dataset(dataset))
    @db.execute(q)

    Util.log 'Setting up tables competed'
  end

  def get_create_statement(name, schema)
    res = "CREATE TABLE IF NOT EXISTS #{name} ("
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
    res += 'WHERE ' + get_primary_key_name(schema) + '= ?;'
    res
  end

  def get_insert_values(schema, object)
    result = []
    schema.each do |schema_element|
      case schema_element[:type]
        when Schema::TYPE_TEXT
          result.push object.attributes[schema_element[:name].to_sym].to_s
        when Schema::TYPE_INTEGER
          result.push object.attributes[schema_element[:name].to_sym]
        when Schema::TYPE_REAL
          result.push object.attributes[schema_element[:name].to_sym].to_i
        when Schema::TYPE_BOOLEAN
          result.push(object.attributes[schema_element[:name].to_sym] ? 1 : 0)
      end

    end
    result.push(Time.now.to_i)
    result
  end

  def get_update_values(schema, object)
    res = get_insert_values(schema, object)
    res.push(get_primary_key_value(schema, object))
    res
  end

  def get_primary_key_name(schema)
    schema.each do |schema_element|
      if schema_element[:primary_key?] then
        return schema_element[:name]
      end
    end
  end

  def get_primary_key_value(schema, object)
    object.attributes[get_primary_key_name(schema).to_sym]
  end

  def add_subreddit(subreddit, mode)
    #if mode == 'bootstrap' || mode == 'incremental'
    #    insert_subreddit subreddit
    #  return
    #end

    #if mode == 'full'
      if !exists?('subreddits', subreddit) then
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
      if !exists?('links', link) then
        insert_link link
      else
        update_link link
      end
      return
    #end
  end

  def bulk_add_links(links, mode)
    links_to_insert = []
    links_to_update = []

    links.each do |link|
      if exists?('links', link) then
        links_to_update.push(link)
      else
        links_to_insert.push(link)
      end
    end

    @db.transaction
      links_to_insert.each do |link|
        insert_link link
      end
    links_to_update.each do |link|
      update_link link
    end
    @db.commit
  end

  def bulk_add_userlinks(userlinks, mode)
    userlinks_to_insert = []
    userlinks_to_update = []

    userlinks.each do |userlink|
      if exists?('userlinks', userlink) then
        userlinks_to_update.push(userlink)
      else
        userlinks_to_insert.push(userlink)
      end
    end

    @db.transaction
    userlinks_to_insert.each do |userlink|
      insert_userlink userlink
    end
    userlinks_to_update.each do |userlink|
      update_userlink userlink
    end
    @db.commit
  end

  def add_userlink(userlink, mode)
    if !exists?('userlinks', userlink) then
      insert_userlink userlink
    else
      update_userlink userlink
    end
  end

  def add_user(user, mode)
    if !exists?('users', user) then
      insert_user user
    else
      update_user user
    end
    return
  end

  def exists?(dataset, object)
    schema = Schema.get_schema_for_dataset dataset
    primary_key_name = get_primary_key_name schema
    primary_key_value = get_primary_key_value(schema, object)

    q = "SELECT #{primary_key_name} FROM #{dataset} WHERE #{primary_key_name} = ?;"
    res = @db.execute(q, primary_key_value)
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

  def insert_userlink(userlink)
    q = get_insert_statement('userlinks', Schema.link_schema)
    v = get_insert_values(Schema.link_schema, userlink)
    @db.execute(q, v)
  end

  def insert_user(user)
    q = get_insert_statement('users', Schema.user_schema)
    v = get_insert_values(Schema.user_schema, user)
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

  def update_userlink(userlink)
    q = get_update_statement('userlinks', Schema.link_schema)
    v = get_update_values(Schema.link_schema, userlink)
    @db.execute(q, v)
  end

  def update_user(user)
    q = get_update_statement('users', Schema.user_schema)
    v = get_update_values(Schema.user_schema, user)
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

  def get_poster_names
    q = 'SELECT DISTINCT author FROM links;'
    q_res = @db.execute(q)

    q_res.map { |row| row[0] }
  end

  def get_all_user_names
    q = 'SELECT name FROM users;'
    q_res = @db.execute(q)

    q_res.map { |row| row[0] }
  end
end
