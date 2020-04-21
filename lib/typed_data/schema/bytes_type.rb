# frozen_string_literal: true

module TypedData
  class Schema
    class BytesType < Type
      def coerce(value, formatter:)
        [value].pack("m0")
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
