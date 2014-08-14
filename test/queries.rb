require 'test/unit'
require 'sqlconstructor'

class SQLConstructorTest < Test::Unit::TestCase

    def test_select1
        assert_equal "SELECT\n col1,col2\nFROM table1\nJOIN table3 USE INDEX (col1)\nON \n(col1 = ccc1)\nJOIN table4,table5\nON \n(col2 = ccc2  AND col3 = 5)\nWHERE \n(col1 = 123  AND col2 IN ('value1','value2','@\#$%^')  AND \n(c1 = 5  AND c2 < 6  OR  (c1 = 3  AND c2 IN (1,2,3,4,5)  AND s1 = 'somestring')))\nLIMIT 100\nUNION\nSELECT\n baz\nFROM table2\nUNION\nSELECT\n fooz\nFROM table5\nLIMIT 20\n\n", 
                     select1
    end

    def select1
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
        sql.to_s
    end

end
