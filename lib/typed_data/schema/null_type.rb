# frozen_string_literal: true

module TypedData
  class Schema
    class NullType < Type
      def primitive?
        true
      end

      def match?(value)
        value.nil?
      end
    end
  end
end
