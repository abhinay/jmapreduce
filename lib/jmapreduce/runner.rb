class Runner
  JAVA_MAIN_CLASS = 'org.fingertap.jmapreduce.JMapReduce'
  
  def initialize(script, input, output, opts={})
    @script = script
    @input = input
    @output = output
    @opts = opts
    
    # env get / set and check
    hadoop_home and hadoop_cmd and hadoop_classpath
  end
  
  def hadoop_home
    ENV['HADOOP_HOME']
  end
  
  def hadoop_cmd
    hadoop = `which hadoop 2>/dev/null`
    hadoop = "#{hadoop_home}/bin/hadoop" if hadoop.empty? and (!hadoop_home.empty?)
    raise 'cannot find hadoop command' if hadoop.empty?
    hadoop.chomp
  end
  
  def hadoop_classpath
    ENV['HADOOP_CLASSPATH'] = ([lib_path] + dirnames + lib_jars).join(':')
  end
  
  def run
    puts cmd
    exec cmd
  end
  
  def cmd
    "#{hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS} #{jars_args} #{file_args} #{conf_args} #{archived_args} #{mapred_args} #{properties_args}"
  end
  
  def jars_args
    "-libjars #{lib_jars.join(',')}"
  end
  
  def file_args
    "-files #{files.join(',')}"
  end
  
  def conf_args
    args = ''
    args += @opts[:conf] ? "-conf #{@opts[:conf]} " : ''
    args += @opts[:namenode] ? "-fs #{@opts[:namenode]} " : ''
    args += @opts[:jobtracker] ? "-jt #{@opts[:jobtracker]} " : ''
    args
  end
  
  def archived_args
    return unless @opts[:dirs]
    
    archived_files = []
    @opts[:dirs].split(',').each do |dir|
      next unless File.directory?(dir)
      tgz = "/tmp/jmapreduce-#{Process.pid}-#{Time.now.to_i}-#{rand(1000)}.tgz"
      system("cd #{dir} && tar -czf #{tgz} *")
      archived_files << "#{tgz}\##{File.basename(dir)}"
    end
    
    "-archives #{archived_files.join(',')}"
  end
  
  def mapred_args
    "#{File.basename(@script)} #{@input} #{@output}"
  end
  
  def properties_args
    return '' if @opts[:properties].nil? && @opts[:json].nil?
    properties = []
    properties << @opts[:properties] if @opts[:properties]
    properties << @opts[:json] if @opts[:json]
    properties.join(',')
  end
  
  def files
    ret = [@script]
    ret += @opts[:files].split(',') if @opts[:files]
    ret
  end
  
  def dirnames
    files.map{ |f| File.dirname(f) }
  end
  
  def lib_jars
    jars = [
      JRubyJars.core_jar_path,
      JRubyJars.stdlib_jar_path,
      main_jar_path,
      File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'vendors', 'gson.jar'))
    ]
    jars += @opts[:libjars].split(',') if @opts[:libjars]
    jars
  end
  
  def main_jar_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'release', 'jmapreduce.jar'))
  end
  
  def lib_path
    File.expand_path(File.join(File.dirname(__FILE__), '..'))
  end
end