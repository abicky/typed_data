# TypedData

![](https://github.com/abicky/ecsmec/workflows/test/badge.svg?branch=master)

TypedData is a library that converts hash objects managed by an Avro schema so that the objects can be loaded into BigQuery.


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'typed_data'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install typed_data

## Usage

```ruby
require "typed_data"

schema = {
  "name" => "Record",
  "type" => "record",
  "fields" => [
    {
      "name" => "int_field",
      "type" => "int",
    },
    {
      "name" => "int_or_string_field",
      "type" => ["int", "string"],
    },
    {
      "name" => "array_field",
      "type" => {
        "type" => "array",
        "items" => "int",
      },
    },
    {
      "name" => "union_type_array_field",
      "type" => {
        "type" => "array",
        "items" => ["int", "string"],
      },
    },
    {
      "name" => "nested_map_field",
      "type" => {
        "type" => "map",
        "values" => {
          "type" => "map",
          "values" => ["int", "string"],
        },
      },
    },
  ],
}

converter = TypedData::Converter.new(schema)
converter.convert({
  "int_field" => 1,
  "int_or_string_field" => "string",
  "array_field" => [1, 2],
  "union_type_array_field" => [1, "2"],
  "nested_map_field" => {
    "nested_map" => {
      "key1" => 1,
      "key2" => "2",
    },
  },
})
#=> {"int_field"=>1,
#    "int_or_string_field"=>{"string_value"=>"string"},
#    "array_field"=>[1, 2],
#    "union_type_array_field"=>[{"int_value"=>"1"}, {"string_value"=>"2"}],
#    "nested_map_field"=>
#     [{"key"=>"nested_map",
#       "value"=>
#        [{"key"=>"key1", "value"=>{"int_value"=>"1"}},
#         {"key"=>"key2", "value"=>{"string_value"=>"2"}}]}]}
```

You can specify a formatter for the union type keys. For example, the formatter for tables managed by [Google BigQuery Sink Connector](https://docs.confluent.io/current/connect/kafka-connect-bigquery/index.html) is like below:

```ruby
converter = TypedData::Converter.new(schema)
converter.union_type_key_formatter = ->(type) { type.split("_").first }
converter.convert({
  "int_field" => 1,
  "int_or_string_field" => "string",
  "array_field" => [1, 2],
  "union_type_array_field" => [1, "2"],
  "nested_map_field" => {
    "nested_map" => {
      "key1" => 1,
      "key2" => "2",
    },
  },
})
#=> {"int_field"=>1,
#    "int_or_string_field"=>{"string"=>"string"},
#    "array_field"=>[1, 2],
#    "union_type_array_field"=>[{"int"=>"1"}, {"string"=>"2"}],
#    "nested_map_field"=>
#     [{"key"=>"nested_map",
#       "value"=>
#        [{"key"=>"key1", "value"=>{"int"=>"1"}},
#         {"key"=>"key2", "value"=>{"string"=>"2"}}]}]}
```


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/abicky/typed_data.


## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
