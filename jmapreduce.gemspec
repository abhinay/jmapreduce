Gem::Specification.new do |s|
  s.homepage    = "https://bitbucket.org/abhinaymehta/jmapreduce"

  s.name        = 'jmapreduce'
  s.version     = '0.1'
  s.date        = "#{Time.now.strftime('%Y-%m-%d')}"

  s.description = "JMapReduce is JRuby Map/Reduce Framework built on top of the Hadoop Distributed computing platform."
  s.summary     = "Map/Reduce Framework"

  s.authors     = ["Abhinay Mehta"]
  s.email       = "abhinay.mehta@gmail.com"
  
  s.add_dependency("jruby-jars")

  s.executables = %w[jmapreduce]

  s.files = %w[
    bin/jmapreduce
    README.md
    lib/jmapreduce/runner.rb
    release/jmapreduce.jar
    vendors/gson.jar
    examples/alice.txt
    examples/wordcount.rb
  ]
end