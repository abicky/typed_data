# frozen_string_literal: true

module TypedData
  class Schema
    class LongType < Type
      SUPPORTED_LOGICAL_TYPES = %w[time-micros timestamp-millis timestamp-micros]

      def accept(visitor, value)
        visitor.visit_long(self, @logical_type, value)
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
        value.is_a?(Integer)
      end
    end
  end
end
