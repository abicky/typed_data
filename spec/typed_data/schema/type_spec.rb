require "spec_helper"

RSpec.describe TypedData::Schema::Type do
  describe "#initialize" do
    context "when the logical type is supported" do
      it do
        expect {
          TypedData::Schema::IntType.new("int", "date")
        }.not_to raise_error
      end
    end

    context "when the logical type is not supported" do
      it do
        expect {
          TypedData::Schema::BooleanType.new("boolean", "unknown")
        }.to raise_error(TypedData::Schema::UnsupportedType)
      end
    end
  end
end
