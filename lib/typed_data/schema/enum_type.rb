# frozen_string_literal: true

module TypedData
  class Schema
    class EnumType < Type
      def initialize(name, symbols)
        @name = name
        @symbols = symbols
      end

      def primitive?
        false
      end

      def match?(value)
        @symbols.include?(value)
      end
    end
  end
end
