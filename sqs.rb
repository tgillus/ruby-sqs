require 'aws-sdk-sqs'

class Queue
  QUEUE_NAME = "ruby-queue"
  REGION = "us-east-1"
  private_constant :QUEUE_NAME, :REGION

  private

  attr_reader :client, :queue_url

  def initialize
    @client = Aws::SQS::Client.new(region: REGION)
    begin
      @queue_url = client.get_queue_url(queue_name: QUEUE_NAME).queue_url
    rescue Aws::SQS::Errors::NonExistentQueue => exception
      raise "Queue named #{QUEUE_NAME} does not exist."
    end
  end

  public

  def push(message:, foo:, baz:)
    client.send_message(
      queue_url: queue_url,
      message_body: message,
      message_attributes: {
        foo: {
          string_value: foo,
          data_type: "String"
        },
        baz: {
          string_value: baz,
          data_type: "Number"
        }
      }
    )
  end

  def process
    puts "Begin receipt of any messages using Aws::SQS::QueuePoller..."
    puts "(Will keep polling until no more messages available for at least 60 seconds.)"

    poller = Aws::SQS::QueuePoller.new(queue_url)
    stats = poller.poll(
      message_attribute_names: ["All"],
      max_number_of_messages: 10,
      idle_timeout: 60
    ) do |messages|
      messages.each do |message|
        puts "Message body: #{message.body}"
        puts "Title: #{message.message_attributes["foo"].string_value}"
        puts "Author: #{message.message_attributes["baz"].string_value}"
      end
    end

    puts "Poller stats:"
    puts "  Polling started at: #{stats.polling_started_at}"
    puts "  Polling stopped at: #{stats.polling_stopped_at}"
    puts "  Last message received at: #{stats.last_message_received_at}"
    puts "  Number of polling requests: #{stats.request_count}"
    puts "  Number of received messages: #{stats.received_message_count}"
  end
end

# puts Queue.new.push(message: "Hello SQS.", foo: "bar", baz: "6")
puts Queue.new.process
