# frozen_string_literal: true

require_relative 'lib/fila/version'

Gem::Specification.new do |spec|
  spec.name = 'fila-client'
  spec.version = Fila::VERSION
  spec.authors = ['Faisca']
  spec.summary = 'Ruby client SDK for the Fila message broker'
  spec.description = 'Idiomatic Ruby client for the Fila message broker using the FIBP binary protocol.'
  spec.homepage = 'https://github.com/faiscadev/fila-ruby'
  spec.license = 'AGPL-3.0-or-later'
  spec.required_ruby_version = '>= 3.1'

  spec.files = Dir['lib/**/*.rb', 'LICENSE', 'README.md']
  spec.require_paths = ['lib']

  spec.metadata['rubygems_mfa_required'] = 'true'
end
