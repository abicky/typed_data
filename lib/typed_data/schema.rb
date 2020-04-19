# frozen_string_literal: true
require "typed_data/schema/array_type"
require "typed_data/schema/boolean_type"
require "typed_data/schema/bytes_type"
require "typed_data/schema/enum_type"
require "typed_data/schema/float_type"
require "typed_data/schema/int_type"
require "typed_data/schema/map_type"
require "typed_data/schema/null_type"
require "typed_data/schema/record_type"
require "typed_data/schema/string_type"
require "typed_data/schema/union_type"

module TypedData
  class Schema
    class UnknownField < StandardError; end
    class UnsupportedType < StandardError; end

    class << self
      def build_type(type, logical_type = nil)
        type = type.first if type.is_a?(Array) && type.size == 1

        case type
        when Array
          UnionType.new(type)
        when Hash
          subtype = type["type"] || type[:type]
          logical_type = type["logicalType"] || type[:logicalType]
          if logical_type
            return build_type(subtype, logical_type)
          end

          case subtype
          when "enum"
            EnumType.new(type["name"] || type[:name], type["symbols"] || type[:symbols])
          when "fixed"
            BytesType.new(type["name"] || type[:name] || "bytes")
          when "array"
            items = type["items"] || type[:items]
            ArrayType.new(items.is_a?(Array) ? items : [items])
          when "map"
            values = type["values"] || type[:values]
            MapType.new(values.is_a?(Array) ? values : [values])
          when "record"
            RecordType.new(type["fields"] || type[:fields])
          else
            raise UnsupportedType, "Unknown type: #{subtype}"
          end
        when "boolean"
          BooleanType.new(type, logical_type)
        when "int", "long"
          IntType.new(type, logical_type)
        when "float", "double"
          FloatType.new(type, logical_type)
        when "bytes"
          BytesType.new(type, logical_type)
        when "string"
          StringType.new(type, logical_type)
        when "null"
          NullType.new(type, logical_type)
        else
          raise UnsupportedType, "Unknown type: #{type}"
        end
      end
    end

    attr_reader :root_type

    # @param schema [Hash] an Avro schema
    def initialize(schema)
      @schema = schema
      if (schema["type"] || schema[:type]) != "record"
        raise UnsupportedType, 'The root type must be "record"'
      end
      @root_type = RecordType.new(schema["fields"] || schema[:fields])
    end
  end
end
