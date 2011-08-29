import 'JMapReduce'

JMapReduce.job 'Count' do |job|
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

JMapReduce.job "Histogram" do |job|
  RANGES = [0..1, 2..3, 4..5, 6..10, 11..20, 21..30, 31..40, 41..50, 51..100, 101..200, 201..300, 301..10_000, 10_001..99_999]

  job.map do |word, count, output|
    range = RANGES.find {|range| range.include?(count.to_i) }
    output << { "#{range.first.to_s.rjust(5,'0')}-#{range.last.to_s.rjust(5,'0')}" => 1 }
  end
  
  job.reduce do |range, counts, output|
    total = counts.inject(0) {|sum,count| sum+count.to_i }
    output << { range => '|'*total }
  end
end