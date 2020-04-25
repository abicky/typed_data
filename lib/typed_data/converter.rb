# frozen_string_literal: true
require "typed_data/schema"

module TypedData
  class Converter
    attr_accessor :union_type_key_formatter

    # @param schema [Hash] an Avro schema
    def initialize(schema)
      @schema = Schema.new(schema)
      @union_type_key_formatter = ->(type) { "#{type}_value" }
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
          converted[key] = subtype.coerce(value, formatter: union_type_key_formatter)
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
          ret << type.coerce(value, formatter: union_type_key_formatter)
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
          value = type.coerce(value, formatter: union_type_key_formatter)
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
        converted_value = convert_array(subtype, value)
      when Schema::MapType
        converted_value = convert_map(subtype, value)
      when Schema::RecordType
        converted_value = convert_record(subtype, value)
      when Schema::UnionType
        converted_value = convert_union(subtype, value)
      else
        return type.coerce(value, formatter: union_type_key_formatter)
      end

      if type.nullable_single?
        converted_value
      else
        { union_type_key_formatter.call(subtype.to_s) => converted_value }
      end
    end
  end
end
