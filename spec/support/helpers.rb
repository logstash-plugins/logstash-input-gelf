# encoding: utf-8
class InfiniteGelfProducer
  def initialize(host, port, chunksize, default_options)
    @client = GELF::Notifier.new(host, port, chunksize, default_options)
  end

  def run
    @producer = Thread.new do
      while true
        @client.notify!("short_message" => "hello world")
      end
    end
  end

  def stop
    @producer.kill
  end
end
