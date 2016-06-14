# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::SLSFlatten < LogStash::Filters::Base

  config_name "sls_flatten"

  config :name_replace, :validate => :string, :default => nil
  config :name_replace_with, :validate => :string, :default => nil
  config :key_pad, :validate => :string, :default => nil
  config :field_target, :validate => :string, :default => "field_target"
  config :skip_fields, :validate => :array, :default => []
  #config :keep_name, :validate => :boolean, :default => false

  public
  def register
  end # def register

  public
  def filter(event)

    root = {}
    data = {}
    event.to_hash.each do |k,v|
      if (v.is_a?(Hash) || v.is_a?(Array)) && (@skip_fields != nil && !skip_fields.include?(k))
        data[k] = v
      else
        root[k] = v
      end
    end

    count = 0

    @key_pad = "" if @key_pad == nil

    data.each do |k,v|
      next if (@skip_fields != nil && @skip_fields.include?(k))

      if v.is_a? Array

        v.each do |hash|
          if hash.is_a? Hash
            new_event = LogStash::Event.new(root.clone)
            new_event[@field_target] = k

            vcount = 0
            hash.each do |key,value|
              new_event[@key_pad+key] = value
              vcount += 1
            end

            if vcount > 0
              count += 1
              filter_matched(new_event)
              yield new_event
            end
          end
        end

      elsif v.is_a? Hash

        v.each do |key_name,hash|
          if hash.is_a? Hash
            name = String.new(key_name)
            name.gsub! @name_replace, @name_replace_with if (@name_replace && @name_replace_with)
            new_event = LogStash::Event.new(root.clone)
            new_event[@field_target] = k

            vcount = 0
            new_event[@key_pad+"name"] = name
            hash.each do |key,value|
              new_event[@key_pad+key] = value
              vcount += 1
            end

            if vcount > 0
              count += 1
              filter_matched(new_event)
              yield new_event
            end
          end
        end
      end

    end

    # Cancel the original event, if other events have been created.
    event.cancel #if count > 0

  end # def filter

  private
  def stuff
  end # def stuff

end # class LogStash::Filters::Example
