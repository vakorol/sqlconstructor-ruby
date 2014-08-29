Gem::Specification.new do |s|
  s.name        = 'sqlconstructor'
  s.version     = '0.1'
  s.date        = '2014-08-29'
  s.summary     = "SQLConstructor"
  s.description = "A gem for constructing custom SQL queries via an object-oriented interface"
  s.authors     = ["Vasiliy Korol"]
  s.email       = 'vakorol@mail.ru'
  s.files       = ["lib/sqlconstructor.rb","lib/sqlobject.rb","lib/sqlexporter.rb","lib/sqlerrors.rb",
                   "lib/sqlconditional.rb","lib/dialects/mysql-constructor.rb",
                   "lib/dialects/mysql-exporter.rb","lib/dialects/example-constructor.rb",
                   "test/queries.rb", "Rakefile", "Readme.md" ]
  s.homepage    = 'https://github.com/vakorol/sqlconstructor-ruby'
  s.license     = 'GPL'
end