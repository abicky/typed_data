# frozen_string_literal: true

module TypedData
  class Schema
    class Type
      def initialize(name, logical_type = nil)
        @name = name
        @logical_type = logical_type
      end

      def to_s
        @name
      end

      def coerce(value, formatter:)
        value
      end

      def primitive?
        raise NotImplementedError, "#{self.class}##{__method__} is not implement"
      end

      def match?(value)
        raise NotImplementedError, "#{self.class}##{__method__} is not implement"
      end
    end
  end
end
