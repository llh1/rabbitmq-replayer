require 'rubygems'
require 'time'
require 'bunny'
require 'optparse'
require 'json'

options = {}
OptionParser.new do |opts|
  opts.on("-f", "--file [FILE]") { |v| options[:file] = v }
  opts.on("-u", "--url [URL]") { |v| options[:url] = v }
  opts.on("-e", "--exchange [EXCHANGE]") { |v| options[:exchange] = v }
end.parse!

abort unless options[:file]
url = options[:url] || 'amqp://localhost:5672'
exchange_name = options[:exchange] || 'psd.s2'

content = File.read(options[:file])
h = JSON.parse(content)

connection = Bunny.new(url)
connection.start
channel = connection.create_channel
channel.prefetch(1)
exchange = channel.topic(exchange_name, :durable => true)

h.each_with_index do |message, i|
  routing_key = message["routing_key"]
  payload = JSON.parse(message["payload"])
  exchange.publish(payload.to_json, {:routing_key => routing_key, :persistent => true})
end

puts "Import done: #{h.size} messages"
