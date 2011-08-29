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
  
  def set_mapreduce(blk)
    self.instance_eval(&blk)
  end
end