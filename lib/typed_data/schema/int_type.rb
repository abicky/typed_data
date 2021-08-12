# frozen_string_literal: true

module TypedData
  class Schema
    class IntType < Type
      VALUE_RANGE = -2**31 .. 2**31 - 1
      SUPPORTED_LOGICAL_TYPES = %w[date time-millis]

      def to_s
        if @logical_type
          "#{@name}_#{@logical_type.gsub("-", "_")}"
        else
          @name
        end
      end

      def coerce(value)
        case @logical_type
        when "date"
          (Date.new(1970, 1, 1) + value).to_s
        when "time-millis"
          Time.at(value / 1_000, value % 1_000 * 1_000).utc.strftime("%T.%3N")
        else
          value
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
