Gem::Specification.new do |s|

  s.name            = 'logstash-input-gelf'
  s.version         = '0.1.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "This input will read GELF messages as events over the network, making it a good choice if you already use Graylog2 today."
  s.description     = "This input will read GELF messages as events over the network, making it a good choice if you already use Graylog2 today."
  s.authors         = ["Elasticsearch"]
  s.email           = 'richard.pijnenburg@elasticsearch.com'
  s.homepage        = "http://logstash.net/"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash', '>= 1.4.0', '< 2.0.0'

  s.add_runtime_dependency 'gelfd', ['0.2.0']
  s.add_runtime_dependency 'logstash-codec-plain'

end

