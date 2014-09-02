Gem::Specification.new do |s|
  s.name        = 'sqlconstructor'
  s.version     = '0.1'
  s.date        = '2014-09-02'
  s.summary     = "SQLConstructor"
  s.description = "A gem for constructing custom SQL queries via an object-oriented interface"
  s.authors     = ["Vasiliy Korol"]
  s.email       = 'vakorol@mail.ru'
  s.files       = Dir.glob("{lib,test,doc}/**/*") + [ "Rakefile", "README.md", "LICENSE.md" ]
  s.homepage    = 'https://github.com/vakorol/sqlconstructor-ruby'
  s.license     = 'MIT'
end
