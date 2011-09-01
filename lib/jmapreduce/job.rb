require 'java'

java_package 'org.fingertap.jmapreduce'

import org.fingertap.jmapreduce.JsonProperty

class JMapReduceJob
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
  
  def set_context(context, key, value)
    @context = context
    @key = key
    @value = value
  end
  
  def set_conf(conf)
    @conf = conf
  end
  
  def conf
    @conf
  end
  
  def emit(key, value)
    @key.set(key.to_s)
    @value.set(value.to_s)
    @context.write(@key, @value)
  end
  
  def set_properties(property)
    return unless property
    
    key,value = *property.split('=')
    if key == 'json'
      @properties = JsonProperty.parse(value)
    else
      @properties ||= {}
      @properties[key] = value
    end
  end
  
  def property(key)
    @properties[key] if @properties
  end
end