# 2.0.7
  - Fix failing test caused by reverting Java Event back to Ruby Event
# 2.0.6
  - Fix plugin crash when Logstash::Json fails to parse a message, https://github.com/logstash-plugins/logstash-input-gelf/pull/27
# 2.0.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash
# 2.0.4
  - New dependency requirements for logstash-core for the 5.0 release
# 2.0.3
 - Fix Timestamp coercion to preserve upto microsecond precision, https://github.com/logstash-plugins/logstash-input-gelf/pull/35
# 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0
