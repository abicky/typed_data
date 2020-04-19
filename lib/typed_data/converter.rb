# frozen_string_literal: true
require "typed_data/schema"

module TypedData
  class Converter
    # @param schema [Hash] an Avro schema
    def initialize(schema)
      @schema = Schema.new(schema)
    end

    # @param data [Hash]
    def convert(data)
      convert_record(@schema.root_type, data)
    end

    private

    # @param type [RecordType]
    # @param record [Hash{String => Object}]
    def convert_record(type, record)
      record.each_with_object({}) do |(key, value), converted|
        subtype = type.find_type(key)
        case subtype
        when Schema::ArrayType
          converted[key] = convert_array(subtype, value)
        when Schema::MapType
          converted[key] = convert_map(subtype, value)
        when Schema::RecordType
          converted[key] = convert_record(subtype, value)
        when Schema::UnionType
          converted[key] = convert_union(subtype, value)
        else
          converted[key] = subtype.coerce(value)
        end
      end
    end

    # @param type [ArrayType]
    # @param array [Array<Object>]
    def convert_array(type, array)
      array.each_with_object([]) do |value, ret|
        next if value.nil?

        subtype = type.find_match(value)
        case subtype
        when Schema::ArrayType
          ret.concat(convert_array(subtype, value))
        when Schema::MapType
          ret << convert_map(subtype, value)
        when Schema::RecordType
          ret << convert_record(subtype, value)
        when Schema::UnionType
          ret << convert_union(subtype, value)
        else
          ret << type.coerce(value)
        end
      end
    end

    # @param type [MapType]
    # @param map [Hash{String => Object}]
    def convert_map(type, map)
      map.each_with_object([]) do |(key, value), ret|
        subtype = type.find_match(value)
        case subtype
        when Schema::ArrayType
          value = convert_array(subtype, value)
        when Schema::MapType
          value = convert_map(subtype, value)
        when Schema::RecordType
          value = convert_record(subtype, value)
        when Schema::UnionType
          value = convert_union(subtype, value)
        else
          value = type.coerce(value)
        end
        ret << { "key" => key, "value" => value }
      end
    end

    # @param type [UnionType]
    # @param map [Object]
    def convert_union(type, value)
      subtype = type.find_match(value)
      case subtype
      when Schema::ArrayType
        type.default_value.merge!("#{subtype}_value" => convert_array(subtype, value))
      when Schema::MapType
        type.default_value.merge!("#{subtype}_value" => convert_map(subtype, value))
      when Schema::RecordType
        type.default_value.merge!("#{subtype}_value" => convert_record(subtype, value))
      when Schema::UnionType
        type.default_value.merge!("#{subtype}_value" => convert_union(subtype, value))
      else
        type.coerce(value)
      end
    end
  end
end
