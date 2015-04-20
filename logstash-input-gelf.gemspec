Gem::Specification.new do |s|

  s.name            = 'logstash-input-gelf'
  s.version         = '0.1.5'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "This input will read GELF messages as events over the network, making it a good choice if you already use Graylog2 today."
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)+::Dir.glob('vendor/*')

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", '>= 1.4.0', '< 2.0.0'

  s.add_runtime_dependency "gelfd", ["0.2.0"]                 #(Apache 2.0 license)
  s.add_runtime_dependency "gelf", ["1.3.2"]                  #(MIT license)
  s.add_runtime_dependency 'logstash-codec-plain'

  s.add_development_dependency 'logstash-devutils'
end

