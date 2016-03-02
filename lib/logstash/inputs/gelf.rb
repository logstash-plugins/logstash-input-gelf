# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/json"
require "logstash/timestamp"
require "stud/interval"
require "date"
require "socket"

# This input will read GELF messages as events over the network,
# making it a good choice if you already use Graylog2 today.
#
# The main use case for this input is to leverage existing GELF
# logging libraries such as the GELF log4j appender. A library used
# by this plugin has a bug which prevents it parsing uncompressed data.
# If you use the log4j appender you need to configure it like this to force
# gzip even for small messages:
#
#   <Socket name="logstash" protocol="udp" host="logstash.example.com" port="5001">
#      <GelfLayout compressionType="GZIP" compressionThreshold="1" />
#   </Socket>
#
#
class LogStash::Inputs::Gelf < LogStash::Inputs::Base
  config_name "gelf"

  default :codec, "plain"

  # The IP address or hostname to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on. Remember that ports less than 1024 (privileged
  # ports) may require root to use.
  config :port, :validate => :number, :default => 12201

  # The GELF protocol (TCP or UDP).
  config :protocol, :validate => :string, :default => "UDP"

  # Whether to capture the hostname or numeric address of the incoming connection
  # Defaults to hostname
  config :use_numeric_client_addr, :validate => :boolean, :default => false

  # Whether or not to remap the GELF message fields to Logstash event fields or
  # leave them intact.
  #
  # Remapping converts the following GELF fields to Logstash equivalents:
  #
  # * `full\_message` becomes `event["message"]`.
  # * if there is no `full\_message`, `short\_message` becomes `event["message"]`.
  config :remap, :validate => :boolean, :default => true

  # Whether or not to remove the leading `\_` in GELF fields or leave them
  # in place. (Logstash < 1.2 did not remove them by default.). Note that
  # GELF version 1.1 format now requires all non-standard fields to be added
  # as an "additional" field, beginning with an underscore.
  #
  # e.g. `\_foo` becomes `foo`
  #
  config :strip_leading_underscore, :validate => :boolean, :default => true

  RECONNECT_BACKOFF_SLEEP = 5

  public
  def initialize(params)
    super
    BasicSocket.do_not_reverse_lookup = true
    @tcp = nil
    @udp = nil
  end # def initialize

  public
  def register
    require 'gelfd'
  end # def register

  public
  def run(output_queue)
    begin
      # udp/tcp server
      if @protocol.downcase == "tcp"
        tcp_listener(output_queue)
      else
        udp_listener(output_queue)
      end
    rescue => e
      unless stop?
        @logger.warn("gelf listener died", :exception => e, :backtrace => e.backtrace)
        Stud.stoppable_sleep(RECONNECT_BACKOFF_SLEEP) { stop? }
        retry unless stop?
      end
    end # begin
  end # def run

  public
  def stop
    if @protocol.downcase == "tcp"
      @tcp.close
    else
      @udp.close
    end
  rescue IOError # the plugin is currently shutting down, so its safe to ignore theses errors
  end

  private
  def tcp_listener(output_queue)
    @logger.info("Starting gelf listener (tcp) ...", :address => "#{@host}:#{@port}")

    if @tcp.nil?
      @tcp = TCPServer.new(@host, @port)
    end

    while !stop?
      Thread.new(@tcp.accept) do |client|
        @logger.debug? && @logger.debug("Gelf (tcp): Accepting connection from:  #{client.peeraddr[2]}:#{client.peeraddr[1]}")

        begin
          while !client.nil? && !client.eof?

            begin # Read from socket
              @data_in = client.gets("\u0000")
            rescue => ex
              @logger.warn("Gelf (tcp): failed gets from client socket:", :exception => ex, :backtrace => ex.backtrace)
            end

             if @data_in.nil?
              @logger.warn("Gelf (tcp): socket read succeeded, but data is nil.  Skipping.")
              next
            end

            # data received.  Remove trailing \0
            @data_in[-1] == "\u0000" && @data_in = @data_in[0...-1]
            begin # Parse JSON
              @jsonObj = JSON.parse(@data_in)
            rescue => ex
              @logger.warn("Gelf (tcp): failed to parse a message. Skipping: " + @data_in, :exception => ex, :backtrace => ex.backtrace)
              next
            end

            begin  # Create event
              event = LogStash::Event.new(@jsonObj)
              event.remove("source_host")
              event["source_host"] = @use_numeric_client_addr && client.addr(:numeric)[3] || client.addr(:hostname)[3]
              if event["timestamp"].is_a?(Numeric)
                event.timestamp = LogStash::Timestamp.at(event["timestamp"])
                event.remove("timestamp")
              end
              remap_gelf(event) if @remap
              strip_leading_underscore(event) if @strip_leading_underscore
              decorate(event)
              output_queue << event
            rescue => ex
              @logger.warn("Gelf (tcp): failed to create event from json object. Skipping: " + @jsonObj.to_s, :exception => ex, :backtrace => ex.backtrace)
            end

          end # while client
          @logger.debug? && @logger.debug("Gelf (tcp): Closing client connection")
          client.close
          client = nil
        rescue => ex
          @logger.warn("Gelf (tcp): client socket failed.", :exception => ex, :backtrace => ex.backtrace)
        ensure
          if !client.nil?
            @logger.debug? && @logger.debug("Gelf (tcp): Ensuring client is closed")
            client.close
            client = nil
          end
        end # begin client
      end  # Thread.new
    end # @!stop?
  end # def tcp_listener

  private
  def udp_listener(output_queue)
    @logger.info("Starting gelf listener (udp)", :address => "#{@host}:#{@port}")

    @udp = UDPSocket.new(Socket::AF_INET)
    @udp.bind(@host, @port)

    while !stop?
      line, client = @udp.recvfrom(8192)
      begin
        data = Gelfd::Parser.parse(line)
      rescue => ex
        @logger.warn("Gelfd failed to parse a message skipping", :exception => ex, :backtrace => ex.backtrace)
        next
      end

      # Gelfd parser outputs null if it received and cached a non-final chunk
      next if data.nil?

      event = LogStash::Event.new(LogStash::Json.load(data))

      event["source_host"] = client[3]
      if event["timestamp"].is_a?(Numeric)
        event.timestamp = LogStash::Timestamp.at(event["timestamp"])
        event.remove("timestamp")
      end
      remap_gelf(event) if @remap
      strip_leading_underscore(event) if @strip_leading_underscore
      decorate(event)

      output_queue << event
    end
  end # def udp_listener

  private
  def remap_gelf(event)
    if event["full_message"] && !event["full_message"].empty?
      event["message"] = event["full_message"].dup
      event.remove("full_message")
      if event["short_message"] == event["message"]
        event.remove("short_message")
      end
    elsif event["short_message"]  && !event["short_message"].empty?
      event["message"] = event["short_message"].dup
      event.remove("short_message")
    end
  end # def remap_gelf

  private
  def strip_leading_underscore(event)
     # Map all '_foo' fields to simply 'foo'
     event.to_hash.keys.each do |key|
       next unless key[0,1] == "_"
       event[key[1..-1]] = event[key]
       event.remove(key)
     end
  end # deef removing_leading_underscores

end # class LogStash::Inputs::Gelf
