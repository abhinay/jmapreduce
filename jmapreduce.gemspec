Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.homepage = "https://bitbucket.org/abhinaymehta/jmapreduce"

  s.name = 'jmapreduce'
  s.version = '0.2'
  s.date = "#{Time.now.strftime('%Y-%m-%d')}"

  s.description = "JMapReduce is JRuby Map/Reduce Framework built on top of the Hadoop Distributed computing platform."
  s.summary     = "Map/Reduce Framework"

  s.authors = ["Abhinay Mehta"]
  s.email = "abhinay.mehta@gmail.com"
  
  s.add_dependency("jruby-jars")

  s.executables = %w[
    jmapreduce
  ]

  # = MANIFEST =
  s.files = %w[
    bin/jmapreduce
    README.rdoc
    lib/jmapreduce/runner.rb
    release/jmapreduce.jar
    vendors/gson.jar
    examples/alice.txt
    examples/wordcount.rb
  ]
  # = MANIFEST =
end