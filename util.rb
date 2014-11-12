class Util
  @silent = false
  @nosleep = false

  class << self
    attr_accessor :silent
    attr_accessor :nosleep
  end

  def self.log(message)
    puts "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')} #{message}" unless silent
    $stdout.flush unless silent
  end

  def self.sleep(seconds)
    Kernel.sleep(seconds) unless nosleep
  end
end
