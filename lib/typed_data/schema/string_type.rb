# frozen_string_literal: true

module TypedData
  class Schema
    class StringType < Type
      SUPPORTED_LOGICAL_TYPES = %w[uuid]

      def primitive?
        true
      end

      def match?(value)
        value.is_a?(String)
      end
    end
  end
end
