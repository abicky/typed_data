# frozen_string_literal: true
require "typed_data/schema/array_type"
require "typed_data/schema/boolean_type"
require "typed_data/schema/bytes_type"
require "typed_data/schema/enum_type"
require "typed_data/schema/float_type"
require "typed_data/schema/int_type"
require "typed_data/schema/long_type"
require "typed_data/schema/map_type"
require "typed_data/schema/null_type"
require "typed_data/schema/record_type"
require "typed_data/schema/string_type"
require "typed_data/schema/union_type"
require "typed_data/schema/errors"

module TypedData
  class Schema
    class << self
      # @param type [String, Hash{Symbol => Object}, Array<Hash{Symbol => Object}>]
      # @param logical_type [String, nil]
      def build_type(type, logical_type = nil)
        type = type.first if type.is_a?(Array) && type.size == 1

        case type
        when Array
          UnionType.new(type)
        when Hash
          actual_type = type[:type]
          if type[:logicalType]
            return build_type(actual_type, type[:logicalType])
          end

          case actual_type
          when "enum"
            EnumType.new(type[:name], type[:symbols])
          when "fixed"
            BytesType.new(type[:name] || "bytes")
          when "array"
            items = type[:items]
            ArrayType.new(items.is_a?(Array) ? items : [items])
          when "map"
            values = type[:values]
            MapType.new(values.is_a?(Array) ? values : [values])
          when "record"
            RecordType.new(type[:name], type[:fields])
          else
            raise UnsupportedType, "Unknown type: #{actual_type}"
          end
        when "boolean"
          BooleanType.new(type, logical_type)
        when "int"
          IntType.new(type, logical_type)
        when "long"
          LongType.new(type, logical_type)
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
      @schema = deep_symbolize_keys(schema)
      @root_type = Schema.build_type(@schema)
    end

    private

    # @param hash [Object]
    # @return [Object] an object with symbolized keys
    def deep_symbolize_keys(o)
      case o
      when Array
        o.map(&method(:deep_symbolize_keys))
      when Hash
        o.each_with_object({}) do |(k, v), h|
          h[k.to_sym] = deep_symbolize_keys(v)
        end
      else
        o
      end
    end
  end
end
