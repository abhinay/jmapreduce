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
  
  java_signature 'void map(java.lang.Object, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    key,value = *value.split("\t") if value.include?("\t")
    
    if @job.mapper.nil?
      @key.set(key.to_s)
      @value.set(value.to_s)
      context.write(@key, @value)
      return
    end
    
    @job.mapper.call(key, value)
  end
end