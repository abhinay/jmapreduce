require 'rubygems'
require 'rake'

namespace :jar do
  task :build do
    `mkdir -p classes`
    `jrubyc -t classes -c vendors/hadoop-core-0.20.2.jar:. --javac lib/*.rb`
  end
end