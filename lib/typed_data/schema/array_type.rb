# frozen_string_literal: true
require "typed_data/schema/type"

module TypedData
  class Schema
    class ArrayType < Type
      attr_reader :element_type

      # @param types [Array<String>]
      def initialize(types)
        @element_type = Schema.build_type(types)
      end

      def accept(visitor, value)
        visitor.visit_array(self, value)
      end

      def to_s
        "array_#{@element_type}"
      end

      def primitive?
        false
      end

      def match?(value)
        value.is_a?(Array) && value.all? { |v| @element_type.match?(v) }
      end
    end
  end
end
