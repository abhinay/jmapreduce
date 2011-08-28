class JMapReduceJob
  def map(blk)
    @mapper = blk
  end

  def reduce(blk)
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
end