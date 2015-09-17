# encoding: utf-8
class InfiniteGelfProducer
  def initialize(host, port, chunksize)
    @client = GELF::Notifier.new(host, port, chunksize)
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
