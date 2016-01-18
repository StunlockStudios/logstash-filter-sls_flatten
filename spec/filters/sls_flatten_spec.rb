# encoding: utf-8
require 'spec_helper'
require "logstash/filters/example"

describe LogStash::Filters::SLSFlatten do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        sls_flatten {
          message => "{}"
        }
      }
    CONFIG
    end

    sample("message" => "{}") do
      expect(subject).to include("message")
      expect(subject['message']).to eq('{}')
    end
  end
end
