# frozen_string_literal: true

module TypedData
  class Schema
    class FloatType < Type
      def primitive?
        true
      end

      def match?(value)
        value.is_a?(Float) || value.is_a?(Integer)
      end
    end
  end
end
