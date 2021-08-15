require "json"
require "typed_data"

schema = JSON.parse(File.read(File.join(__dir__, "schema.avsc")))
data = JSON.parse(File.read(File.join(__dir__, "converted_data.jsonl")))

puts "Schema:"
pp schema
puts

puts "Input data:"
pp data
puts

restorer = TypedData::Restorer.new(schema)
puts "Restored data:"
pp restorer.restore(data)
