require 'rubygems'

class JRunner
  JAVA_MAIN_CLASS = 'JMapReduce'

  attr_reader :script, :files

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
    "#{hadoop_cmd} jar #{main_jar_path} #{JAVA_MAIN_CLASS} #{jars_args} #{file_args} #{conf_args} #{mapred_args}"
  end

  def jars_args
    "-libjars #{lib_jars.join(',')}"
  end

  def file_args
    files = [@script]
    "-files #{files.join(',')}"
  end

  def conf_args
    @opts[:conf] ? "-conf #{@opts[:conf]}" : ''
  end

  def mapred_args
    "#{File.basename(@script)} #{@input} #{@output}"
  end
  
  def dirnames
    [File.dirname(@script)]
  end

  def lib_jars
    [File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'vendors', 'jruby.jar')), main_jar_path]
  end

  def main_jar_path
    File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'release', 'jmapreduce.jar'))
  end

  def lib_path
    File.expand_path(File.join(File.dirname(__FILE__), '..'))
  end
end