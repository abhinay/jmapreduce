require 'rubygems'
require 'rake'

namespace :jar do
  task :build do
    `mkdir -p classes`
    `jrubyc -t classes -c vendors/hadoop-core-0.20.2.jar:. --javac lib/jmapreduce.rb lib/jmapper.rb lib/jreducer.rb lib/job.rb`
    `javac -d classes -cp vendors/jruby-1.6.3.jar:vendors/hadoop-core-0.20.2.jar:classes/:. lib/MapperWrapper.java lib/ReducerWrapper.java`

    `mkdir -p release`
    `jar cvf release/jmapreduce.jar -C classes/ .`
  end
  
  task :clean do
    `rm -rf classes`
    `rm -rf release`
  end
end