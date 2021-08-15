require "json"
require "typed_data"

schema = JSON.parse(File.read(File.join(__dir__, "schema.avsc")))
data = JSON.parse(File.read(File.join(__dir__, "data.jsonl")))

puts "Schema:"
pp schema
puts

puts "Input data:"
pp data
puts

converter = TypedData::Converter.new(schema)
puts "Converted data with the default key formatter:"
pp converter.convert(data)
puts

converter = TypedData::Converter.new(schema, key_formatter: :avro)
puts "Converted data with the key formatter :avro:"
pp converter.convert(data)
