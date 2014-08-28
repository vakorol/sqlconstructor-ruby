require 'test/unit'
require 'sqlconstructor'

class SQLConstructorTest < Test::Unit::TestCase

    def test_select1
        assert_equal "SELECT\n col1,col2\nFROM table1\nJOIN table3 USE INDEX (col1)\nON \n(col1 = ccc1)\nJOIN table4,table5\nON \n(col2 = ccc2  AND col3 = 5)\nWHERE \n(col1 = 123  AND col2 IN ('value1','value2','@\#$%^')  AND \n(c1 = 5  AND c2 < 6  OR  (c1 = 3  AND c2 IN (1,2,3,4,5)  AND s1 = 'somestring')))\nLIMIT 100\nUNION\nSELECT\n baz\nFROM table2\nUNION\nSELECT\n fooz\nFROM table5\nLIMIT 20\n\n", 
                     select1
    end

    def test_select2
        assert_equal "SELECT\n t.id,t.tag,c.title category\nFROM tags2Articles t2a\nINNER JOIN tags t\nON \n(t.id = t2a.idTag)\nINNER JOIN categories c\nON \n(t.tagCategory = c.id)\nINNER JOIN \n(SELECT\n a.id\nFROM articles a\nJOIN tags2articles ta\nON \n(a.id = ta.idArticle)\nJOIN tags tsub\nON \n(ta.idTag = tsub.id)\nWHERE \n(tsub.id IN (12,13,16))\nGROUP BY a.id\nHAVING \n(COUNT(DISTINCT tsub.id) = 3)\n) asub\nON \n(t2a.idArticle = asub.id)\n",
                     select2
    end

    def test_insert1
        assert_equal "INSERT\nINTO table2\n SELECT\n name,CONCAT('blah=',ID)\nFROM table1\n\n",
                     insert1
    end

    def test_delete1
        assert_equal "DELETE\nFROM keywords\nWHERE \n(keyword_id IN \n(SELECT\n id\nFROM \n(SELECT\n k.keyword id\nFROM keywords k\nWHERE \n(k.keyword_type = 'CAMPAIGN'  AND k.keyword != 'Airtel'  AND k.keyword != 'Nokia'  AND k.keyword != 'Micromax'  AND k.keyword NOT IN \n(SELECT\nDISTINCT\n keyword_id\nFROM customer_analysis\n))\nORDER BY k.keyword_id\n) a\n))\n",
                     delete1
    end

    def test_update1
        assert_equal "UPDATE\n guest g\nSET link_id=\n(SELECT\n MAX(h.host_id)\nFROM guest_data d\nINNER JOIN host_data h\nON \n(d.guest_nm = hhost_nm)\nWHERE \n(d.guest_id = g.guest_id)\nGROUP BY h.venue_nm\nHAVING \n(COUNT(*) = 1)\n)\nWHERE \n(g.guest_id IN \n(SELECT\n d.guest_id\nFROM guest_data d\nINNER JOIN host_data h\nON \n(d.guest_nm = hhost_nm)\nGROUP BY h.venue_nm\nHAVING \n(COUNT(*) = 1)\n))\n",
                     update1
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

    def select2
        sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
        sql.select( :"t.id",:"t.tag",:"c.title" => :category ).from( :tags2Articles => :t2a )
        sql.inner_join( :tags => :t ).on.eq(:"t.id", :"t2a.idTag" )
        sql.inner_join( :categories => :c ).on.eq( :"t.tagCategory", :"c.id" )
        inner_sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
        inner_sql.select( :"a.id" ).from( :articles => :a )
        inner_sql.join( :tags2articles => :ta ).on.eq( :"a.id", :"ta.idArticle" )
        inner_sql.join( :tags => :tsub ).on.eq( :"ta.idTag", :"tsub.id" )
        inner_sql.where.in( :"tsub.id", [12,13,16] ).group_by( :"a.id" ).
                  having.eq( :"COUNT(DISTINCT tsub.id)", 3 )
        sql.inner_join( inner_sql => 'asub' ).on.eq( :"t2a.idArticle", :"asub.id" )
        sql.to_s
    end

    def insert1
        sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
        sql.insert.into(:table2).select(:name, :"CONCAT('blah=',ID)").from(:table1)
        sql.to_s
    end

    def delete1
        sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
        inner_sel3 = SQLConstructor.new( :tidy => true, :dialect => 'mysql' ).select( :keyword_id ).distinct.from( :customer_analysis )
        inner_sel2 = SQLConstructor.new( :tidy => true, :dialect => 'mysql' ).select( :"k.keyword" => :id ).from( :keywords => :k ).
                     where.eq( :"k.keyword_type", "CAMPAIGN" ).and.ne( :"k.keyword", "Airtel" ).
                     and.ne( :"k.keyword", "Nokia" ).and.ne( :"k.keyword", "Micromax" ).
                     and.not_in( :"k.keyword", inner_sel3 ).order_by( :"k.keyword_id" )
        inner_sel1 = SQLConstructor.new( :tidy => true, :dialect => 'mysql' ).select( :id ).from( inner_sel2 => :a )
        sql.delete.from( :keywords ).where.in( :keyword_id, inner_sel1 )
        sql.to_s
    end

    def update1
        sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
        in_sel1 = SQLConstructor.new( :tidy => true )
        in_sel1.select( :"MAX(h.host_id)" ).from( :guest_data => :d ).
                  inner_join( :host_data => :h ).on.eq( :"d.guest_nm", :hhost_nm ).where. 
                  eq( :"d.guest_id", :"g.guest_id" ).group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
        in_sel2 = SQLConstructor.new( :dialect => 'mysql', :tidy => true )
        in_sel2.select( :"d.guest_id" ).from( :guest_data => :d ).
                  inner_join( :host_data => :h ).on.eq( :"d.guest_nm", :hhost_nm ).
                  group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
        sql.update( :guest => :g ).set( :link_id => in_sel1).where.in( :"g.guest_id", in_sel2 )
        sql.to_s
    end
 
end
