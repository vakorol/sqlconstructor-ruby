
DIALECTS_PATH = File.expand_path( "../dialects", __FILE__ )
 
require File.expand_path( "../sqlobject", __FILE__ )
require File.expand_path( "../sqlconditional", __FILE__ )
require File.expand_path( "../sqlexporter", __FILE__ )
require File.expand_path( "../sqlerrors", __FILE__ )

##################################################################################################
#   Author::    Vasiliy Korol  (mailto:vakorol@mail.ru)
#   Copyright:: Vasiliy Korol (c) 2014
#   License::   Distributes under terms of GPLv2
#
#   This class implements methods to construct a valid SQL query.
#   SQL SELECT, DELETE, UPDATE and INSERT clauses are supported. 
#
#   There's also an experimental implementation of MySQL index hints.
#
#   Column values and other data that should be escaped is passed to the methods as strings. 
#   Column and table names, aliases and everything that goes unescaped is passed as symbols.
#   === Typical usage:
#       sql = SQLConstructor.new
#       sql.select( :col1, :col2 ).from( :table ).where.eq( :col3, 16 ).and.lt( :col4, 5 )
#       p sql
#
#   will result in:
#
#       SELECT  col1,col2 FROM table WHERE  (col3 = 16  AND col4 < 5)
#
#   One can also construct complex queries like:
#
#       sql = SQLConstructor.new( :tidy => true, :dialect => 'mysql' )
#       inner_select1 = SQLConstructor.new( :tidy => true )
#       inner_select1.select( :"MAX(h.item_id)" ).from( :item_data => :d ).
#         inner_join( :call_data => :h ).on.eq( :"d.item_nm", :call_ref ).where.
#             eq( :"d.item_num", :"g.item_num" ).group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
#       inner_select2 = SQLConstructor.new( :dialect => 'mysql', :tidy => true )
#       inner_select2.select( :"d.item_num" ).from( :item_data => :d ).
#             inner_join( :call_data => :h ).on.eq( :"d.item_nm", :call_ref ).
#             group_by( :"h.venue_nm" ).having.eq( :"COUNT(*)", 1 )
#       sql.update( :guest => :g ).set( :link_id => inner_select1).
#           where.in( :"g.item_num", inner_select2 )
#       p sql
#
#   It will produce:
#
#       UPDATE
#        guest g
#       SET link_id=
#       (SELECT
#        MAX(h.item_id)
#       FROM item_data d
#       INNER JOIN call_data h
#       ON 
#       (d.item_nm = call_ref)
#       WHERE 
#       (d.item_num = g.item_num)
#       GROUP BY h.venue_nm
#       HAVING 
#       (COUNT(*) = 1)
#       )
#       WHERE 
#       (g.item_num IN 
#       (SELECT
#        d.item_num
#       FROM item_data d
#       INNER JOIN call_data h
#       ON 
#       (d.item_nm = call_ref)
#       GROUP BY h.venue_nm
#       HAVING 
#       (COUNT(*) = 1)
#       ))
#
#   Queries can be modified "on the fly", which can be useful for dynamic construction:
#
#       sql.delete.from( :datas ).where.ne( :x, "SOME TEXT" ).order_by( :y )
#       p sql
#
#       DELETE
#       FROM datas
#       WHERE 
#       (x != 'SOME TEXT')
#       ORDER BY y
#
#       sql._remove( :order_by )
#       sql._get( :from ).push( :dataf )
#       p sql
#
#       DELETE
#       FROM datas,dataf
#       WHERE 
#       (x != 'SOME TEXT')
#################################################################################################
class SQLConstructor < SQLObject

    attr_accessor :exporter, :tidy
    attr_reader   :obj, :dialect

     # Dirty hack to make .join work on an array of SQLConstructors
    alias :to_str :to_s
 
    ##########################################################################
    #   Class constructor. Accepts an optional argument with a hash of
    #   parameters :dialect and :tidy to set the SQLExporter object in @exporter,
    #   or :exporter to receive a predefined SQLExporter object.
    ##########################################################################
    def initialize ( params = nil )
        @dialect, @string, @obj, @tidy = nil, nil, nil, false
        if params.is_a? Hash
            @dialect  = params[ :dialect  ]
            @tidy     = params[ :tidy     ]
            @exporter = params[ :exporter ]
        end
        @exporter ||= SQLExporter.new @dialect, @tidy
        @dialect = @exporter.dialect
    end
    
    ##########################################################################
    #   Add a SELECT statement with columns specified by *cols.
    #   Returns an instance of BasicSelect_[%dialect%] class.
    ##########################################################################
    def select ( *cols )
        _getGenericQuery 'select', *cols
    end

    ##########################################################################
    #   Add a DELETE statement.
    #   Returns an instance of BasicDelete_[%dialect%] class.
    ##########################################################################
    def delete
        _getGenericQuery 'delete'
    end

    ##########################################################################
    #   Add a INSERT statement
    #   Returns an instance of BasicInsert_[%dialect%] class.
    ##########################################################################
    def insert
        _getGenericQuery 'insert'
    end

    ##########################################################################
    #   Add a UPDATE statement
    #   Returns an instance of BasicUpdate_[%dialect%] class.
    ##########################################################################
    def update ( *tabs )
        _getGenericQuery 'update', *tabs
    end
   
    ##########################################################################
    #   Convert object to string by calling the .export() method of
    #   the @exporter object.
    ##########################################################################
    def to_s
#        return @string  if @string
        @obj.inline = self.inline
        @string = @exporter.export @obj
    end

    ##########################################################################
    #   Pass all unknown methods to @obj or throw an exception if the call
    #   already originated from @obj.
    ##########################################################################
    def method_missing ( method, *args )
        return @obj.send( method, *args )  if @obj && @obj.child_caller != @obj  
         # raise an exception if the call is "bouncing" between self and @obj
        raise NoMethodError, ERR_UNKNOWN_METHOD + 
            ": '#{method.to_s}' from #{@obj.class.name}"
    end
 

  #########
  private
  #########

    ##########################################################################
    #   Returns an instance of Basic* child dialect-specific class 
    ##########################################################################
    def _getGenericQuery ( type, *args )
        class_basic = 'Basic' + type.capitalize
        class_child = class_basic + '_' + @dialect
        begin
            @obj = self.class.const_get( class_child ).new self, *args
        rescue NameError
            @obj = self.class.const_get( class_basic ).new self, *args
        end
    end
 
 
  ###############################################################################################
  ###############################################################################################
    class QAttr
        
        attr_reader :name, :text, :val_type, :type, :no_commas
        attr_accessor :val

        def initialize ( init_hash = nil )
            if init_hash.is_a? Hash
                @name           = init_hash[:name]
                @text           = init_hash[:text]
                @val            = init_hash[:val]
                @val_type       = init_hash[:val_type]
                @type           = init_hash[:type]
                @no_commas      = init_hash[:no_commas]
            end
        end


        def to_s
            if [ SQLValList, SQLAliasedList ].include? @val
                result = @val.to_s
            else 
                result = @text
                if @val
                    val_arr = @val.is_a?( Array )  ? @val  : [ @val ]
                    result += " " + val_arr.join( "," )
                end
            end
            return result
        end

    end


  ###############################################################################################
  #   Internal class - generic query attributes and methods. Should be parent to all Basic*
  #   classes.
  ###############################################################################################
    class GenericQuery < SQLObject

        attr_accessor :caller, :string
        attr_reader :type, :dialect, :exporter, :child_caller, :tidy, :attr_index_hints

         # Dirty hack to make .join work on an array of GenericQueries
        alias :to_str :to_s
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            @caller   = _caller
            @dialect  = @caller.dialect
            @tidy     = @caller.tidy
            @exporter = _caller.exporter
            @inline   = @caller.inline
            self._setMethods
        end

        ##########################################################################
        #   Returns an object by clause (keys of child class' METHODS attribute)
        #   or by SQLObject.name
        ##########################################################################
        def _get ( clause, *args )
            result = nil
            if @methods.has_key? clause
                name = args  ? args[0]  : nil
                result = self.send @methods[clause].name
                result = result.val  if result.is_a? QAttr
                if name && [ Array, SQLValList, SQLAliasedList, SQLCondList ].include?( result.class )
                     # return the first object if multiple objects have the same name
                    result = result.find { |obj|  obj.name == name }
                end
            end                         
            return result
        end
 
        ##########################################################################
        #   NILs attribute by clause name (specified in the child class' METHODS 
        #   attribure), or removes an named item from a list attribute.
        #   This method must be overriden in child classes if any methods were 
        #   defined explicitly (not in METHODS).
        ##########################################################################
        def _remove ( clause, *args )
            if @methods.has_key? clause
                _attr = self.send @methods[clause].name
                name = args  ? args[0]  : nil
                if name && [ Array, SQLValList, SQLAliasedList, SQLCondList ].include?( _attr.class )
                    _attr.delete_if { |obj|  obj.name == name }
                else
                    self.send "#{@methods[clause].name}=", nil
                end
                @string = nil
            end                         
            return self
        end

        ##########################################################################
        #   Convert object to string by calling the .export() method of
        #   the @exporter object.
        ##########################################################################
        def to_s
            return @string  if @string
            @string = @exporter.export self
        end
 
        ##########################################################################
        #   Process method calls described in the child's METHODS attribute.
        #   If no corresponding entries are found in all object's parent classes,
        #   then send missing methods calls to the @caller object.
        ##########################################################################
        def method_missing ( method, *args )
             # If the method is described in the class' METHODS constant, then
             # create an attribute with the proper name, an attr_accessor
             # for it, and set it's value to the one in METHODS.
            if @methods.has_key? method
                _attr = @methods[method].dup
                attr_name = _attr.name
                val_obj = nil

                 # get the current value of the attribute {_attr.name}
                self.class.send :attr_accessor, attr_name.to_sym
                cur_attr = self.send attr_name.to_sym
                cur_attr_val = cur_attr.is_a?( QAttr )  ? cur_attr.val  : cur_attr
 
                 # Create an instance of the corresponding class if _attr.val is 
                 # on of SQLObject container classes:
                if [ SQLValList, SQLAliasedList, SQLCondList ].include? _attr.val
                    _attr.val = _attr.val.new *args

                 # Create an array of SQLObjects if _attr.val is SQLObject class:
                elsif _attr.val == SQLObject
                    _attr.val = SQLObject.get *args #args.map{ |arg|  SQLObject.get arg }

                 # Create an instance of the corresponding class if _attr.val is 
                 # SQLConstructor or SQLConditional class:
                elsif [ SQLConstructor, SQLConditional ].include? _attr.val
                    val_obj = cur_attr_val || _attr.val.new(
                                                    :dialect  => @dialect,
                                                    :tidy     => @tidy,
                                                    :exporter => @exporter,
                                                    :caller   => self
                                              )
                    _attr.val = val_obj

                 # create a BasicSelect dialect-specific child class: 
                elsif _attr.val == BasicSelect
                    val_obj = SQLConstructor.new( 
                                                    :dialect  => @dialect, 
                                                    :tidy     => @tidy,
                                                    :exporter => @exporter 
                                                ).select( *args )
                    _attr.val = val_obj

                 # If the :val parameter is some different class, then we should 
                 # create an instance of it or return the existing value:
                elsif _attr.val && _attr.val.ancestors.include?( GenericQuery )
                    val_obj = _getBasicClass( _attr.val, _attr.text )
                    _attr.val = val_obj
                end

                 # If the object already has attribute {_attr.name} defined and it's
                 # an array or one of the SQL* class containers,then we should rather 
                 # append to it than reassign the value.
                 # If :attr_val=list, then create a new SQLAliasedList container.
                if [ Array, SQLValList, SQLAliasedList, SQLCondList ].include?( cur_attr_val.class ) ||
                   _attr.val_type == 'list'
                    cur_attr_val ||= SQLAliasedList.new
                    cur_attr_val.no_commas = true  if _attr.no_commas
                    cur_attr_val.push _attr.val
                    _attr = cur_attr_val
                end

#                self.class.send :attr_accessor, attr_name.to_sym  if ! cur_attr_val
                self.send "#{attr_name}=", _attr

                @string = nil
                return ( val_obj || self )
            end

             # Otherwise send the call to @caller object
            @child_caller = self
            return @caller.send( method.to_sym, *args )  if @caller
            raise NoMethodError, ERR_UNKNOWN_METHOD + ": " + method.to_s
        end

      ###########
      protected
      ###########

        ##########################################################################
        #   Creates a new BasicJoin object for the JOIN statement.
        ##########################################################################
        def _addJoin ( type, *tables )
            @string = nil
            join = _getBasicClass BasicJoin, type, *tables
            @attr_joins ||= [ ]
            @attr_joins.push join
            return join
        end

        ##########################################################################
        #   Returns an instance of Basic* child dialect-specific class 
        ##########################################################################
        def _getBasicClass ( class_basic, *args )
            class_basic_name = class_basic.name.sub /^(?:\w+::)*/, '' 
            class_child = class_basic_name + '_' + @dialect
            if SQLConstructor.const_defined? class_child.to_sym
                SQLConstructor.const_get( class_child.to_sym ).new self, *args
            else
                SQLConstructor.const_get( class_basic_name.to_sym ).new self, *args
            end
        end

        ##########################################################################
        #   Returns the METHODS hash of child dialect-specific class merged with
        #   parent's METHODS hash.
        ##########################################################################
        def _setMethods
            if ! @methods
                methods_self = { }
                self.class.ancestors.each do |_class| 
                    next  if ! _class.ancestors.include? SQLConstructor::GenericQuery
                    begin
                        class_methods = _class.const_get :METHODS || { }
                    rescue
                        class_methods = { }
                    end
                    methods_self.merge! class_methods
                end
                @methods = methods_self
            end
            return @methods
        end
  
    end


  ###############################################################################################
  #   Internal class which represents a basic JOIN statement.
  ###############################################################################################
    class BasicJoin < GenericQuery

        attr_accessor :join_on, :join_sources, :join_using

        METHODS = {
            :on    => QAttr.new( :name => 'join_on',    :text => 'ON',    :val => SQLConditional ),
            :using => QAttr.new( :name => 'join_using', :text => 'USING', :val => SQLObject      ),
        }
 
        ##########################################################################
        #   Class contructor. Takes a caller object as the first argument, JOIN 
        #   type as the second argument, and a list of sources for the JOIN clause
        ##########################################################################
        def initialize ( _caller, type, *sources )
            type = type.to_s
            type.upcase!.gsub! /_/, ' '
            super _caller
            @type = type
            @join_sources = SQLAliasedList.new *sources
        end

        ##########################################################################
        #   Adds more sources to @join_sources list
        ##########################################################################
        def join_more ( *sources )
            @join_sources.push *sources
        end

        ##########################################################################
        #   Export to string with sources aliases
        ##########################################################################
        def to_s
            return @string  if @string
            result  = @type + " "
            arr = [ ]
            @join_sources.each do |src|
                _alias = src.alias ? " " + src.alias.to_s : ""
                str = src.to_s + _alias
                arr << str
            end
            result += arr.join ','
            result += @exporter.separator
            result += "ON " + @join_on.val.to_s  if @join_on
            @string = result
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic UNION statement.
  ###############################################################################################
    class BasicUnion < GenericQuery

        ##########################################################################
        #   Class contructor. Takes a caller object as the first argument and UNION 
        #   type as the second argument. Inits @obj to new SQLConstructor instance
        ##########################################################################
        def initialize ( _caller, type )
            @type = type
            super _caller
            @obj = SQLConstructor.new( :dialect => @dialect, :tidy => @tidy )
        end

        ##########################################################################
        #   Export to string 
        ##########################################################################
        def to_s
            @type + @caller.exporter.separator + @obj.to_s
        end

        ##########################################################################
        #   Override GenericQuery method and send call to @obj
        ##########################################################################
        def _get ( *args )
            @obj._get *args
        end

        ##########################################################################
        #   Override GenericQuery method and send call to @obj
        ##########################################################################
        def _remove ( *args )
            @obj._remove *args
        end
 
        ##########################################################################
        #   Send call to @obj
        ##########################################################################
        def method_missing ( method, *args )
            @obj.send method, *args
        end

    end
 

  ###############################################################################################
  #   Internal class which represents a basic SELECT statement.
  ###############################################################################################
    class BasicSelect < GenericQuery

        attr_accessor :attr_expression, :attr_group_by, :attr_unions, :attr_index_hints, 
                      :attr_distinction, :attr_having, :attr_group_by_order, :attr_where, :attr_from,
                      :attr_first, :attr_skip, :attr_order_by, :attr_order_by_order, :attr_joins

         # Hash - list of available class meta-methods, which would be processed by .method_missing()
         # to set the appropriate object's attributes (as defined in the METHODS hash itself).
         # The keys of the hash are the methods names (symbols), the values are instances of
         # the QAttr class.
        METHODS = {
            :where => QAttr.new( :name => 'attr_where', :text => 'WHERE', :val => SQLConditional ),
            :from  => QAttr.new( :name => 'attr_from',  :text => 'FROM',  :val => SQLAliasedList ),
            :all         => QAttr.new( :name => 'attr_distinction', :text => 'ALL'         ),
            :distinct    => QAttr.new( :name => 'attr_distinction', :text => 'DISTINCT'    ),
            :distinctrow => QAttr.new( :name => 'attr_distinction', :text => 'DISTINCTROW' ),
            :having => QAttr.new( :name => 'attr_having', :text => 'HAVING', :val => SQLConditional ),
            :group_by => QAttr.new( :name => 'attr_group_by', :text => 'GROUP BY', :val => SQLObject),
            :group_by_asc   => QAttr.new( :name => 'attr_group_by_order', :text => 'ASC'  ),
            :group_by_desc  => QAttr.new( :name => 'attr_group_by_order', :text => 'DESC' ),
            :union          => QAttr.new( 
                                :name     => 'attr_unions',   
                                :text     => 'UNION',
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :union_all      => QAttr.new(
                                :name     => 'attr_unions',
                                :text     => 'UNION_ALL',
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :union_distinct => QAttr.new(
                                :name     => 'attr_unions',   
                                :text     => 'UNION_DISTINCT', 
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :join => SQLConstructor::QAttr.new( :name => "attr_joins", :text => "JOIN", 
                                                :val => SQLConstructor::BasicJoin, 
                                                :val_type => 'list' ),
            :first    => QAttr.new( :name => 'attr_first', :text => 'FIRST', :val => SQLObject ),
            :skip     => QAttr.new( :name => 'attr_skip',  :text => 'SKIP',  :val => SQLObject ),
            :order_by => QAttr.new( :name => 'attr_order_by', :text => 'ORDER BY', :val => SQLObject ),
            :order_by_asc  => QAttr.new( :name => 'attr_order_by_order', :text => 'ASC' ),
            :order_by_desc => QAttr.new( :name => 'attr_order_by_order', :text => 'DESC' )
        }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @attr_expression = QAttr.new(
                                :name => 'attr_expression',
                                :text => '',
                                :val  => SQLAliasedList.new( *list )
                              )
        end

        ##########################################################################
        #   Add more objects to SELECT expression list ( @attr_expression[:val] )
        ##########################################################################
        def select_more ( *list )
            @attr_expression.val.push *list
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic DELETE statement.
  ###############################################################################################
    class BasicDelete < GenericQuery

        attr_accessor :del_using, :attr_where, :attr_from, :attr_skip, :attr_first, :attr_order_by,
                      :attr_order_by_order

        METHODS = { 
                :using => QAttr.new( :name => 'del_using', :text => 'USING', :val => SQLObject ),
                :where => QAttr.new( :name => 'attr_where', :text => 'WHERE', :val => SQLConditional),
                :from  => QAttr.new( :name => 'attr_from',  :text => 'FROM',  :val => SQLAliasedList),
                :first => QAttr.new( :name => 'attr_first', :text => 'FIRST', :val => SQLObject ),
                :skip  => QAttr.new( :name => 'attr_skip',  :text => 'SKIP',  :val => SQLObject ),
                :order_by => QAttr.new( :name => 'attr_order_by', :text => 'ORDER BY', 
                                        :val => SQLObject ),
                :order_by_asc  => QAttr.new( :name => 'attr_order_by_order', :text => 'ASC' ),
                :order_by_desc => QAttr.new( :name => 'attr_order_by_order', :text => 'DESC' )
                  }

        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            super
        end

    end 


  ###############################################################################################
  #   Internal class which represents a basic INSERT statement.
  ###############################################################################################
    class BasicInsert < GenericQuery

        attr_reader :ins_into, :ins_values, :ins_set, :ins_columns, :ins_select

        METHODS = {
                :into => QAttr.new( :name => 'ins_into', :text => 'INTO', :val => SQLObject ),
                :values => QAttr.new( :name => 'ins_values', :text => 'VALUES', :val => SQLValList),
                :set => QAttr.new( :name => 'ins_set', :text => 'SET', :val => SQLCondList ),
                :columns => QAttr.new( :name => 'ins_columns', :text => 'COLUMNS', :val => SQLObject),
                :select => QAttr.new( :name => 'ins_select', :text => '', :val => BasicSelect )
                  }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            super
        end

    end 
 

  ###############################################################################################
  #   Internal class which represents a basic INSERT statement.
  ###############################################################################################
    class BasicUpdate < GenericQuery

        attr_accessor :upd_tables, :upd_set, :attr_where, :attr_order_by, :attr_first, :attr_skip

        METHODS = {
                :tables => QAttr.new( :name => 'upd_tables', :text => '', :val => SQLObject ),
                :set    => QAttr.new( :name => 'upd_set', :text => 'SET', :val => SQLCondList ),
                :where => QAttr.new( :name => 'attr_where', :text => 'WHERE', :val => SQLConditional),
                :first  => QAttr.new( :name => 'attr_first', :text => 'FIRST', :val => SQLObject ),
                :skip   => QAttr.new( :name => 'attr_skip',  :text => 'SKIP',  :val => SQLObject ),
                  }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @upd_tables = QAttr.new( :name => 'upd_tables', :text => '',
                                     :val  => SQLAliasedList.new( *list ) )
        end

        ##########################################################################
        #   Add tables to UPDATE tables list ( @upd_tables[:val] )
        ##########################################################################
        def update_more ( *list )
            @upd_tables.val.push *list
        end

    end 
 
end
 

##################################################################################################
##################################################################################################
#   Include dialect-specific classes from ./dialects/constructor/  :
#   This should be done after SQLConstructor is defined.
##################################################################################################
##################################################################################################

Dir[ DIALECTS_PATH + "/*-constructor.rb"].each { |file|  require file }

