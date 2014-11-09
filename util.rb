class Util
  def self.log(message)
    puts "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')} #{message}"
    $stdout.flush
  end
end
