import org.fingertap.jmapreduce.JMapReduce

JMapReduce.job 'Count' do
  reduce_tasks 1
  
  map do |key, value|
    value.split.each do |word|
      emit(word, 1)
    end
  end
  
  reduce do |key, values|
    sum = 0
    values.each {|v| sum += v }
    emit(key, {'sum' => sum})
  end
end

JMapReduce.job "Histogram" do
  setup do
    RANGES = [0..1, 2..3, 4..5, 6..10, 11..20, 21..30, 31..40, 41..50, 51..100, 101..200, 201..300, 301..10_000, 10_001..99_999]
  end
  
  map do |word, count|
    range = RANGES.find {|range| range.include?(count['sum']) }
    emit("#{range.first.to_s.rjust(5,'0')}-#{range.last.to_s.rjust(5,'0')}", 1)
  end
  
  reduce do |range, counts|
    total = counts.inject(0) {|sum,count| sum+count }
    emit(range, '|'*(total/20))
  end
end

# this job is just a pass though which takes advantage of the map/reduce shuffle to get ordered keys
JMapReduce.job "Sort" do
  reduce_tasks 1
end

__END__

./bin/jmapreduce examples/wordcount.rb examples/alice.txt /tmp/output