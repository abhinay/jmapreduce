require 'rubygems'
require 'rake'

namespace :jar do
  task :build do
    `mkdir -p classes`
    `jrubyc -t classes -c vendors/hadoop-core-0.20.2.jar:. --javac lib/*.rb`

    `mkdir -p release`
    `jar cvf release/jmapreduce.jar -C classes/ .`
  end
  
  task :clean do
    `rm -rf classes`
    `rm -rf release`
  end
end