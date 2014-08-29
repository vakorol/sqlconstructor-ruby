sqlconstructor-ruby
===================

SQLConstructor - Ruby gem for constructing SQL queries via object-oriented API.
Currently only MySQL dialect is supported. Hopefully, Informix syntax will be added, too.
It is easy to add support for mostly any SQL dialect - see files lib/dialects/example-constructor.rb
and lib/dialects/example-exporter.rb.

SQL SELECT, DELETE, UPDATE and INSERT clauses are supported. There's also an experimental 
implementation of MySQL index hints.

Column values and other data that should be escaped is passed to the methods as strings. Column
and table names, aliases and everything that goes unescaped is passed as symbols.

Detailed rdoc class documentation can be found in the doc folder.

Typical usage:

```ruby
sql = SQLConstructor.new
sql.select( :col1, :col2 ).from( :table ).where.eq( :col3, 16 ).and.lt( :col4, 5 )
p sql
```

will result in:
```
SELECT  col1,col2 FROM table WHERE  (col3 = 16  AND col4 < 5)
```

One can also construct complex queries like:

```ruby
    sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
    inner_select1 = SQLConstructor.new( :tidy => true )
    inner_select1.select( :"MAX(h.item_id)" ).from( :item_data => :d ).
          inner_join( :call_data => :h ).on.eq( :"d.item_nm", :call_ref ).where.
          eq( :"d.item_num", :"g.item_num" ).group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
    inner_select2 = SQLConstructor.new( :dialect => 'mysql', :tidy => true )
    inner_select2.select( :"d.item_num" ).from( :item_data => :d ).
          inner_join( :call_data => :h ).on.eq( :"d.item_nm", :call_ref ).
          group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
    sql.update( :guest => :g ).set( :link_id => inner_select1).
        where.in( :"g.item_num", inner_select2 )
    p sql
```
    will produce:
```
    UPDATE
     guest g
    SET link_id=
    (SELECT
     MAX(h.item_id)
    FROM item_data d
    INNER JOIN call_data h
    ON 
    (d.item_nm = call_ref)
    WHERE 
    (d.item_num = g.item_num)
    GROUP BY h.venue_nm
    HAVING 
    (COUNT(*) = 1)
    )
    WHERE 
    (g.item_num IN 
    (SELECT
     d.item_num
    FROM item_data d
    INNER JOIN call_data h
    ON 
    (d.item_nm = call_ref)
    GROUP BY h.venue_nm
    HAVING 
    (COUNT(*) = 1)
    ))
```
Queries can be modified "on the fly", which can be useful for dynamic construction:
```ruby
    sql.delete.from( :datas ).where.ne( :x, "SOME TEXT" ).order_by( :y )
    p sql
```
```
    DELETE
    FROM datas
    WHERE 
    (x != 'SOME TEXT')
    ORDER BY y
```
```ruby
    sql._remove( :order_by )
    sql._get( :from ).push( :dataf )
    p sql
```
```
    DELETE
    FROM datas,dataf
    WHERE 
    (x != 'SOME TEXT')
```

The list of available methods for the SQL queries:

|SELECT         |DELETE       |INSERT    |UPDATE  |
|---------------|-------------|----------|--------|
|select         |delete       |insert    |tables  |
|from           |from         |into      |set     |
|where          |where        |values    |where   |
|select_more    |using        |set       |first   |
|having         |order_by     |columns   |skip    |
|distinct       |order_by_asc |select    |        |
|all            |order_by_desc|          |        |
|distinctrow    |first        |          |        |
|join           |skip         |          |        |
|group_by       |             |          |        |
|group_by_asc   |             |          |        |
|group_by_desc  |             |          |        |
|order_by       |             |          |        |
|order_by_asc   |             |          |        |
|order_by_desc  |             |          |        |
|first          |             |          |        |
|skip           |             |          |        |
|union          |             |          |        |   
|union_all      |             |          |        |   
|union_distinct |             |          |        |   
                                                     
MySQL-specific methods:                              

|SELECT              |DELETE      |INSERT                 |UPDATE      |
|--------------------|------------|-----------------------|------------|
|straight_join       |low_priority|low_priority           |low_priority|
|sql_cache           |quick       |delayed                |ignore      |
|sql_no_cache        |ignore      |high_priority          |limit       |
|high_priority       |limit       |quick                  |            |
|sql_calc_found_rows |            |ignore                 |            |
|sql_small_result    |            |on_duplicate_key_update|            |
|sql_big_result      |            |                       |            |
|sql_buffer_result   |            |                       |            |
|limit               |            |                       |            |
|group_by_with_rollup|            |                       |            |
|use_index           |            |                       |            |
|force_index         |            |                       |            |
|ignore_index        |            |                       |            |
|use_key             |            |                       |            |
|force_key           |            |                       |            |
|ignore_key          |            |                       |            |
