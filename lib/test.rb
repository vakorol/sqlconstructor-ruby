#!/usr/bin/ruby

require_relative "sqlconstructor"
require_relative "sqlconditional"

sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
#sql.select(":col1",":col2").from("table1").where.eq(":col1", 123).and.in(":col2",["value1","value2","@#\$%^"])
#sql.limit( 100 )
#sql.union.select('baz').from("table2")
#c2 = SQLConditional.new.eq(':c1',3).and.in(':c2',[1,2,3,4,5]).and.eq(':s1','somestring')
#c1 = SQLConditional.new.eq(':c1',5).and.lt(':c2',6).or.is(c2)
#sql.where.and.is(c1)
#sql.join( 'table3' ).on.eq( ":col1", ":ccc1" ).use_index( ":col1" )
#sql.join( 'table4','table5' ).on.eq( ":col2", ":ccc2").and.eq( ":col3", 5 )

sql.delete.from('t1').where.eq(':b',5)

#sql.insert.into(':tab').values('12','13','14')
p sql#.obj.ins_values


