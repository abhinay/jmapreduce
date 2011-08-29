require 'java'
import 'JMapReduce'

JMapReduce.job 'Name' do |job|
  job.map do |key, value, output|
    value.split.each do |word|
      output << { word => 1 }
    end
  end
  
  job.reduce do |key, values, output|
    sum = 0
    values.each {|v| sum += v.to_i }
    output << { key => sum }
  end
end