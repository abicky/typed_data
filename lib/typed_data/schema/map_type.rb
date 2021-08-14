# frozen_string_literal: true

module TypedData
  class Schema
    class MapType < Type
      attr_reader :element_type

      # @param types [Array<String>]
      def initialize(types)
        @element_type = Schema.build_type(types)
      end

      def accept(visitor, value)
        visitor.visit_map(self, value)
      end

      def to_s
        "map_#{@element_type}"
      end

      def primitive?
        false
      end

      def match?(value)
        value.is_a?(Hash) && value.all? { |_, v| @element_type.match?(v) }
      end
    end
  end
end
