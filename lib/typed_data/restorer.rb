# frozen_string_literal: true
require "time"
require "typed_data/schema"

module TypedData
  class Restorer
    attr_accessor :union_type_key_formatter

    # @param schema [Hash] an Avro schema
    def initialize(schema)
      @schema = Schema.new(schema)
      @union_type_key_formatter = ->(type) { "#{type}_value" }
    end

    # @param data [Hash]
    def restore(data)
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
      value.unpack("m0").first
    end

    # @param type [TypedData::Schema::IntType]
    # @param logical_type [String, nil] a logical type of the int type
    # @param value [Integer]
    def visit_int(type, logical_type, value)
      case logical_type
      when "date"
        (Date.parse(value) - Date.new(1970, 1, 1)).to_i
      when "time-millis"
        t = Time.parse(value)
        (t.sec + t.min * 60 + t.hour * 60**2) * 10**3 + t.nsec / 10**6
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
        t = Time.parse(value)
        (t.sec + t.min * 60 + t.hour * 60**2) * 10**6 + t.nsec / 10**3
      when "timestamp-millis"
        t = parse_as_utc(value)
        t.to_i * 10**3 + t.nsec / 10**6
      when "timestamp-micros"
        t = parse_as_utc(value)
        t.to_i * 10**6 + t.nsec / 10**3
      else
        value
      end
    end

    # @param type [TypedData::Schema::RecordType]
    # @param record [Hash{String => Object}]
    def visit_record(type, record)
      record.each_with_object({}) do |(key, value), restored|
        restored[key] = type.find_type(key).accept(self, value)
      end
    end

    # @param type [TypedData::Schema::ArrayType]
    # @param array [Array<Object>]
    def visit_array(type, array)
      array.each_with_object([]) do |value, ret|
        next if value.nil?

        if type.element_type.is_a?(Schema::ArrayType)
          # BigQuery doesn't support nested arrays
          ret << type.element_type.element_type.accept(self, value)
        else
          ret << type.element_type.accept(self, value)
        end
      end
    end

    # @param type [TypedData::Schema::MapType]
    # @param map [Hash{String => Object}]
    def visit_map(type, array)
      array.each_with_object({}) do |hash, ret|
        ret[hash["key"]] = type.element_type.accept(self, hash["value"])
      end
    end

    # @param type [TypedData::Schema::UnionType]
    # @param types [Array<TypedData::Schema::Type>] types the union type includes
    # @param map [Object]
    def visit_union(type, types, value)
      if type.nullable_single?
        return if value.nil?

        element_type = types.find { |t| !t.is_a?(Schema::NullType) }
        element_type.accept(self, value)
      else
        value_without_nil = value.compact
        return if value_without_nil.empty?

        k = value_without_nil.keys.first
        v = value_without_nil.values.first
        element_type = types.find { |t| k == union_type_key_formatter.call(t.to_s) }
        element_type.accept(self, v)
      end
    end

    private

    # @param time [String]
    def parse_as_utc(time)
      d = Date._parse(time)
      Time.utc(d[:year], d[:mon], d[:mday], d[:hour], d[:min], d[:sec], d.fetch(:sec_fraction, 0) * 1000000)
    end
  end
end
