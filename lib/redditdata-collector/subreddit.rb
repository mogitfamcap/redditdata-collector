module Subreddit
  class << self
    def name_to_url(name)
      '/r/' + name
    end

    def url_to_name(url)
      url.sub('/r/', '').sub('/', '')
    end
  end
end