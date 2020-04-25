# frozen_string_literal: true

module TypedData
  class Schema
    class IntType < Type
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
        when "time-micros"
          Time.at(value / 1_000_000, value % 1_000_000).utc.strftime("%T.%6N")
        when "timestamp-millis"
          Time.at(value / 1_000, value % 1_000 * 1_000).utc.strftime("%F %T.%3N")
        when "timestamp-micros"
          Time.at(value / 1_000_000, value % 1_000_000).utc.strftime("%F %T.%6N")
        else
          value
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
