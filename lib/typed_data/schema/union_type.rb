# frozen_string_literal: true

module TypedData
  class Schema
    class UnionType < Type
      # @param types [Array<String>]
      def initialize(types)
        @types = types.map(&Schema.method(:build_type))
        @nullable_single = @types.size == 2 && @types.any? { |t| t.is_a?(NullType) }
        @nullable_primitive = @nullable_single && @types.any?(&:primitive?)
      end

      def to_s
        @nullable_primitive ? @types.first.to_s : "union_#{@types.map(&:to_s).join("_")}"
      end

      def coerce(value, formatter:)
        return value if @nullable_primitive

        type = find_match(value)
        if type.is_a?(NullType)
          {}
        else
          { formatter.call(type.to_s) => type.coerce(value, formatter: formatter).to_s }
        end
      end

      def primitive?
        false
      end

      def find_match(value)
        @types.find { |t| t.match?(value) }
      end

      def match?(value)
        @types.any? { |t| t.match?(value) }
      end

      def nullable_single?
        @nullable_single
      end
    end
  end
end
