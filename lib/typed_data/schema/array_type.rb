# frozen_string_literal: true
require "typed_data/schema/type"

module TypedData
  class Schema
    class ArrayType < Type
      attr_reader :fields

      # @param types [Array<String>]
      def initialize(types)
        @type = Schema.build_type(types.select { |t| t != "null" })
      end

      def to_s
        "array_#{@type}"
      end

      def primitive?
        false
      end

      def find_match(value)
        @type.match?(value) ? @type : @type.find_match(value)
      end

      def match?(value)
        value.is_a?(Array) && value.all? { |v| @type.match?(v) }
      end
    end
  end
end
