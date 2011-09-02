require 'java'

java_package 'org.fingertap.jmapreduce'

import org.fingertap.jmapreduce.JsonProperty

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable

class JMapReduceJob
  def initialize
    @key = Text.new
    @value = Text.new
    @text_value = Text.new
  end
  
  def setup(&blk)
    @setup = blk
  end
  
  def map(&blk)
    @mapper = blk
  end
  
  def reduce(&blk)
    @reducer = blk
  end
  
  def mapper
    @mapper
  end
  
  def reducer
    @reducer
  end
  
  def get_setup
    @setup
  end
  
  def setup_exists
    !@setup.nil?
  end
  
  def set_name(name)
    @name = name
  end
  
  def name
    @name
  end
  
  def map_tasks(num_of_tasks)
    @map_tasks = num_of_tasks
  end
  
  def reduce_tasks(num_of_tasks)
    @reduce_tasks = num_of_tasks
  end
  
  def num_of_reduce_tasks
    return @reduce_tasks if @reduce_tasks
    @reducer ? 1 : 0
  end
  
  def set_mapreduce(blk)
    self.instance_eval(&blk)
  end
  
  def set_context(context)
    @context = context
  end
  
  def set_conf(conf)
    @conf = conf
  end
  
  def conf
    @conf
  end
  
  def emit(key, value)
    @key.set(key.to_s)
    if value.is_a?(String)
      @text_value.set(value)
      @context.write(@key, @text_value)
    else
      set_value(value)
      @context.write(@key, @value)
    end
  end
  
  def set_properties(properties)
    return unless properties
    
    @properties = {}
    props = properties.split(',')
    props.each do |property|
      key,value = *property.split('=')
      if key == 'json'
        JsonProperty.parse(value).each do |(k,v)|
          @properties[k] = v
        end
      else
        @properties[key] = value
      end
    end
  end
  
  def property(key)
    @properties[key] if @properties
  end
  
  def set_value(value)
    case @value_type
    when :int, :float then @value.set(value)
    when :array then @value.set(JsonProperty.array_to_json(value.map { |v| v.to_s }.to_java(:string)))
    when :hash then @value.set(JsonProperty.hash_to_json(java.util.HashMap.new(value)))
    else @value.set(value.to_s)
    end
  end
  
  def get_value(value)
    case @value_type
    when :int, :float then return value.get
    when :array then return JsonProperty.array_from_json(value.to_s).to_a
    when :hash then return JsonProperty.hash_from_json(value.to_s)
    else return value.to_s
    end
  end
  
  def value_type(type)
    @value_type = type
    case @value_type
    when :int then @value = IntWritable.new
    when :float then @value = FloatWritable.new
    when :string, :array, :hash then @value = @value
    else raise "value type not recognised: #{type}"
    end
  end
  
  def value_class
    @value.java_class
  end
end