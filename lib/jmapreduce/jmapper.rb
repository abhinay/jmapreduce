require 'java'
require File.join(File.dirname(__FILE__), 'common')

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class JMapper < Mapper
  include Common
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    super
  end
  
  java_signature 'void map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    
    if value.include?("\t")
      tokens = value.split("\t")
      key = tokens.first
      value = tokens[1..-1].join("\t")
    end
    
    if @job.mapper.nil?
      @key.set(key.to_s)
      @value.set(value.to_s)
      context.write(@key, @value)
      return
    end
    
    @job.mapper.call(key, @job.unpack(value))
  end
end