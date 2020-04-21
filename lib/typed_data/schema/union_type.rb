# frozen_string_literal: true

module TypedData
  class Schema
    class UnionType < Type
      # @param types [Array<String>]
      def initialize(types)
        @types = types.map(&Schema.method(:build_type))
        @nullable_primitive = @types.size == 2 && @types.any?(&:primitive?) && @types.any? { |t| t.is_a?(NullType) }
      end

      def to_s
        @nullable_primitive ? @types.first.to_s : "union_#{@types.map(&:to_s).join("_")}"
      end

      def coerce(value, formatter:)
        return value if @nullable_primitive

        type = find_match(value)
        if type.is_a?(NullType)
          default_value(formatter)
        else
          default_value(formatter).merge!(formatter.call(type.to_s) => type.coerce(value, formatter: formatter).to_s)
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

      def default_value(formatter)
        @types.each_with_object({}) do |t, v|
          next if t.is_a?(NullType)
          v[formatter.call(t.to_s)] = t.primitive? || t.is_a?(EnumType) ? nil : []
        end
      end
    end
  end
end
