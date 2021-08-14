# frozen_string_literal: true

module TypedData
  class Schema
    class BytesType < Type
      def accept(visitor, value)
        visitor.visit_bytes(self, value)
      end

      def primitive?
        true
      end

      def match?(value)
        value.is_a?(String)
      end
    end
  end
end
