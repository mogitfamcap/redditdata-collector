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

  def add_item(dataset, object)
    if !exists?(dataset, object) then
      insert_item(dataset, object)
    else
      update_item(dataset, object)
    end
  end

  def bulk_add_items(dataset, objects)
    items_to_insert = []
    items_to_update = []

    objects.each do |object|
      if exists?(dataset, object) then
        items_to_update.push(object)
      else
        items_to_insert.push(object)
      end
    end

    @db.transaction
    items_to_insert.each do |object|
      insert_item(dataset, object)
    end
    items_to_update.each do |object|
      update_item(dataset, object)
    end
    @db.commit
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

  def get_all_users_with_userlinks
    q = 'SELECT DISTINCT author FROM userlinks;'
    q_res = @db.execute(q)

    q_res.map { |row| row[0] }
  end

  private

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
    res += 'WHERE ' + Schema.get_primary_key_name(schema) + '= ?;'
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
    res.push(Schema.get_primary_key_value(schema, object))
    res
  end

  def exists?(dataset, object)
    schema = Schema.get_schema_for_dataset dataset
    primary_key_name = Schema.get_primary_key_name schema
    primary_key_value = Schema.get_primary_key_value(schema, object)

    q = "SELECT #{primary_key_name} FROM #{dataset} WHERE #{primary_key_name} = ?;"
    res = @db.execute(q, primary_key_value)
    !res.empty?
  end

  def insert_item(dataset, object)
    q = get_insert_statement(dataset, Schema.get_schema_for_dataset(dataset))
    v = get_insert_values(Schema.get_schema_for_dataset(dataset), object)
    @db.execute(q, v)
  end

  def update_item(dataset, object)
    q = get_update_statement(dataset, Schema.get_schema_for_dataset(dataset))
    v = get_update_values(Schema.get_schema_for_dataset(dataset), object)
    @db.execute(q, v)
  end
end
