require 'rubygems'
require 'rake'

namespace :jar do
  task :build do
    `mkdir -p classes`
    `jrubyc -t classes -c vendors/hadoop.jar:. --javac lib/jmapreduce.rb lib/jmapreduce/jmapper.rb lib/jmapreduce/jreducer.rb lib/jmapreduce/job.rb`
    `javac -d classes -cp vendors/jruby.jar:vendors/hadoop.jar:classes/:. lib/jmapreduce/MapperWrapper.java lib/jmapreduce/ReducerWrapper.java`
    
    `mkdir -p release`
    `jar cvf release/jmapreduce.jar -C classes/ .`
  end
  
  task :clean do
    `rm -rf classes`
    `rm -rf release`
  end
end