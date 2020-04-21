require "bundler/setup"
require "typed_data"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  if ENV["GOOGLE_APPLICATION_CREDENTIALS"] .nil? || ENV["BIGQUERY_DATASET"].nil?
    config.filter_run_excluding bigquery: true
  end
end
