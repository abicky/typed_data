# frozen_string_literal: true

module TypedData
  class Schema
    class MapType < Type
      # @param types [Array<String>]
      def initialize(types)
        @type = Schema.build_type(types)
      end

      def to_s
        "map_#{@type}"
      end

      def coerce(value)
        @type.coerce(value)
      end

      def primitive?
        false
      end

      def find_match(value)
        @type.match?(value) ? @type : @type.find_match(value)
      end

      def match?(value)
        value.is_a?(Hash) && value.all? { |_, v| @type.match?(v) }
      end
    end
  end
end
