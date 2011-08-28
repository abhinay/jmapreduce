require 'java'
import 'JMapReduce'

JMapReduce.job 'Name' do |job|
  job.map do |value|
    return [{value => value}]
  end
  
  job.reduce do |key, values|
    return [{key => values.first.to_s}]
  end
end