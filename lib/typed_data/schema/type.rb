# frozen_string_literal: true
require "typed_data/schema/errors"

module TypedData
  class Schema
    class Type
      SUPPORTED_LOGICAL_TYPES = []

      def initialize(name, logical_type = nil)
        @name = name
        if logical_type && !self.class::SUPPORTED_LOGICAL_TYPES.include?(logical_type)
          raise UnsupportedType, %Q{#{name} doesn't support the logical type "#{logical_type}"}
        end
        @logical_type = logical_type
      end

      def accept(visitor, value)
        visitor.visit(self, value)
      end

      def to_s
        @name
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
