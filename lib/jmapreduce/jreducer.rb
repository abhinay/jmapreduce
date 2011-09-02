require 'java'
require File.join(File.dirname(__FILE__), 'common')

java_package 'org.fingertap.jmapreduce'

import java.io.IOException

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class JReducer < Reducer
  include Common
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def setup(context)
    super
  end
  
  java_signature 'void reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context) throws IOException'
  def reduce(key, values, context)
    if @job.reducer.nil?
      values.each do |value|
        context.write(key, value)
      end
      return
    end
    
    @job.reducer.call(key, values.map{|v|v.to_s})
  end
end