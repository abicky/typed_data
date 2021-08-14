# frozen_string_literal: true

module TypedData
  class Schema
    class IntType < Type
      VALUE_RANGE = -2**31 .. 2**31 - 1
      SUPPORTED_LOGICAL_TYPES = %w[date time-millis]

      def accept(visitor, value)
        visitor.visit_int(self, @logical_type, value)
      end

      def to_s
        if @logical_type
          "#{@name}_#{@logical_type.gsub("-", "_")}"
        else
          @name
        end
      end

      def primitive?
        true
      end

      def match?(value)
        value.is_a?(Integer) && VALUE_RANGE.cover?(value)
      end
    end
  end
end
