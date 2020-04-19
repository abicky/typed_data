require_relative 'lib/typed_data/version'

Gem::Specification.new do |spec|
  spec.name          = "typed_data"
  spec.version       = TypedData::VERSION
  spec.authors       = ["abicky"]
  spec.email         = ["takeshi.arabiki@gmail.com"]

  spec.summary       = %q{A library that converts hash objects managed by an Avro schema}
  spec.description   = %q{TypedData is a library that converts hash objects managed by an Avro schema so that the objects can be loaded into BigQuery.}
  spec.homepage      = "https://github.com/abicky/typed_data"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/abicky/typed_data"

  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "avro"
  spec.add_development_dependency "google-cloud-bigquery"
end
