Gem::Specification.new do |s|
  s.name        = 'sqlconstructor'
  s.version     = '0.0.0'
  s.date        = '2014-08-14'
  s.summary     = "SQLConstructor"
  s.description = "A class for constructing and executing custom SQL queries"
  s.authors     = ["Vasiliy Korol"]
  s.email       = 'vakorol@mail.ru'
  s.files       = ["lib/sqlconstructor.rb","lib/sqlobject.rb","lib/sqlexporter.rb","lib/sqlerrors.rb",
                   "lib/sqlconditional.rb","lib/dialects/mysql-constructor.rb",
                   "lib/dialects/mysql-exporter.rb","lib/dialects/example-constructor.rb"]
  s.homepage    = 'https://github.com/vakorol/sqlconstructor-ruby'
  s.license     = 'GPL'
end