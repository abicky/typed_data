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
      @schema.root_type.accept(self, data)
    end

    # @param type [TypedData::Schema::Type]
    # @param value [Object]
    def visit(type, value)
      value
    end

    # @param type [TypedData::Schema::BytesType]
    # @param value [String]
    def visit_bytes(type, value)
      [value].pack("m0")
    end

    # @param type [TypedData::Schema::IntType]
    # @param logical_type [String, nil] a logical type of the int type
    # @param value [Integer]
    def visit_int(type, logical_type, value)
      case logical_type
      when "date"
        (Date.new(1970, 1, 1) + value).to_s
      when "time-millis"
        Time.at(value / 1_000, value % 1_000 * 1_000).utc.strftime("%T.%3N")
      else
        value
      end
    end

    # @param type [TypedData::Schema::LongType]
    # @param logical_type [String, nil] logical type of the long type
    # @param value [Integer]
    def visit_long(type, logical_type, value)
      case logical_type
      when "time-micros"
        Time.at(value / 1_000_000, value % 1_000_000).utc.strftime("%T.%6N")
      when "timestamp-millis"
        Time.at(value / 1_000, value % 1_000 * 1_000).utc.strftime("%F %T.%3N")
      when "timestamp-micros"
        Time.at(value / 1_000_000, value % 1_000_000).utc.strftime("%F %T.%6N")
      else
        value
      end
    end

    # @param type [TypedData::Schema::RecordType]
    # @param record [Hash{String => Object}]
    def visit_record(type, record)
      record.each_with_object({}) do |(key, value), converted|
        converted[key] = type.find_type(key).accept(self, value)
      end
    end

    # @param type [TypedData::Schema::ArrayType]
    # @param array [Array<Object>]
    def visit_array(type, array)
      array.each_with_object([]) do |value, ret|
        next if value.nil?

        converted_value = type.element_type.accept(self, value)
        if type.element_type.is_a?(Schema::ArrayType)
          # BigQuery doesn't support nested arrays
          ret.concat(converted_value)
        else
          ret << converted_value
        end
      end
    end

    # @param type [TypedData::Schema::MapType]
    # @param map [Hash{String => Object}]
    def visit_map(type, map)
      map.each_with_object([]) do |(key, value), ret|
        ret << { "key" => key, "value" => type.element_type.accept(self, value) }
      end
    end

    # @param type [TypedData::Schema::UnionType]
    # @param types [Array<TypedData::Schema::Type>] types the union type includes
    # @param map [Object]
    def visit_union(type, types, value)
      element_type = types.find { |t| t.match?(value) }
      if element_type.nil?
        raise Schema::InvalidValue, %Q{the value #{value.inspect} doesn't match the type #{types.map(&:to_s)}}
      end
      converted_value = element_type.accept(self, value)

      if type.nullable_single?
        converted_value
      elsif element_type.is_a?(Schema::NullType)
        {}
      else
        { union_type_key_formatter.call(element_type.to_s) => converted_value }
      end
    end
  end
end
