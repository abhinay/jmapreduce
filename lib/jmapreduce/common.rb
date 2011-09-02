module Common
  def setup(context)
    @key = Text.new
    @value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    
    require script
    @job = JMapReduce.jobs[job_index]
    @job.set_context(context, @key, @value)
    @job.set_conf(conf)
    @job.get_setup.call if @job.setup_exists
    @job.set_properties(conf.get('jmapreduce.property'))
  end
end