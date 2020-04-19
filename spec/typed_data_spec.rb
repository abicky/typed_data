require "spec_helper"

RSpec.describe TypedData do
  it "has a version number" do
    expect(TypedData::VERSION).not_to be nil
  end
end
