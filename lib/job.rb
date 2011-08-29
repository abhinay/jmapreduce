class JMapReduceJob
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
end