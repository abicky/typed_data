# frozen_string_literal: true

module TypedData
  class Schema
    class BooleanType < Type
      def primitive?
        true
      end

      def match?(value)
        value == true || value == false
      end
    end
  end
end
