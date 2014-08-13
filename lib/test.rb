#!/usr/bin/ruby

require_relative "sqlconstructor"
require_relative "sqlconditional"

sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
sql.select(:col1,:col2).from(:table1).where.eq(:col1, 123).and.in(:col2,["value1","value2","@#\$%^"])
sql.limit( 100 )
sql.union._name("u1").select(:baz).from(:table2)
c2 = SQLConditional.new.eq(:c1,3).and.in(:c2,[1,2,3,4,5]).and.eq(:s1,'somestring')
c1 = SQLConditional.new.eq(:c1,5).and.lt(:c2,6).or.is(c2)
sql.where.and.is(c1)
sql.join( :table3 )._name('j1').on.eq( :col1, :ccc1 ).use_index( :col1 )
sql.join( :table4,:table5 )._name('j2').on.eq( :col2, :ccc2).and.eq( :col3, 5 )
sql.union._name("u2").select(:fooz).from(:table5).limit( 20 )

#p sql.sel_unions[0][:val].class

#p sql.sel_unions

#ql._remove( :union, "u1" )
#sql.delete.from('t1').where.eq(':b',5)

#sql.insert.into(:tab).values('12','13','14')

#sql.update(:tab).set( :x => 12, :y => :DEFAULT)
#sql._remove( :union, 'u1' )#.find{ |obj| obj.name == 'u1' }#.obj.ins_values

j1 = sql._get( :join, 'j1')
j1._string = j1._string.sub /col1/, "col1,col2"

p sql


