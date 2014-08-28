
DIALECTS_PATH = File.expand_path( "../dialects", __FILE__ )
 
require File.expand_path( "../sqlobject", __FILE__ )
require File.expand_path( "../sqlconditional", __FILE__ )
require File.expand_path( "../sqlexporter", __FILE__ )
require File.expand_path( "../sqlerrors", __FILE__ )

##################################################################################################
#   This class implements methods to construct a valid SQL query.
#
#   Author::    Vasiliy Korol  (mailto:vakorol@mail.ru)
#   Copyright:: Vasiliy Korol (c) 2014
#   License::   Distributes under terms of GPLv2
##################################################################################################
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
    #   Add a SELECT statement with columns specified by *cols 
    ##########################################################################
    def select ( *cols )
        _getGenericQuery 'select', *cols
    end

    ##########################################################################
    #   Add a DELETE statement
    ##########################################################################
    def delete
        _getGenericQuery 'delete'
    end

    ##########################################################################
    #   Add a INSERT statement
    ##########################################################################
    def insert
        _getGenericQuery 'insert'
    end

    ##########################################################################
    #   Add a UPDATE statement
    ##########################################################################
    def update ( *tabs )
        _getGenericQuery 'update', *tabs
    end
   
    ##########################################################################
    #   Pass all unknown methods to @obj
    ##########################################################################
    def method_missing ( method, *args )
        return @obj.send( method, *args )  if @obj && @obj.child_caller != @obj  
         # raise an exception if the call is "bouncing" between self and @obj
        raise NoMethodError, ERR_UNKNOWN_METHOD + 
            ": '#{method.to_s}' from #{@obj.class.name}"
    end
    
    ##########################################################################
    #   Convert object to string by calling the .export() method of
    #   the @exporter object.
    ##########################################################################
    def to_s
        return @string  if @string
        @obj.inline = self.inline
        @string = @exporter.export @obj
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
  #   classes
  ###############################################################################################
    class GenericQuery < SQLObject

        attr_accessor :caller
        attr_reader :type, :dialect, :exporter, :child_caller, :tidy, :gen_index_hints

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
        #   Returns an object by clause (keys of METHODS hash) or by SQLObject.name
        ##########################################################################
        def _get ( clause, *args )
            name = args  ? args[0]  : nil
            result = nil
            if @methods.has_key? clause
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
        #   NILs attribute by clause name (specified in the METHODS constant), or
        #   removes an named item from a list attribute.
        #   This method must be overriden in child classes if any methods were 
        #   defined explicitly (not in METHODS).
        ##########################################################################
        def _remove ( clause, *args )
            name = args  ? args[0]  : nil
            result = nil
            if @methods.has_key? clause
                _attr = self.send @methods[clause].name
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
            return @string
        end
 
        ##########################################################################
        #   Process method calls for clauses described in METHODS constant array
        #   of the calling object's class.
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
            @gen_joins ||= [ ]
            @gen_joins.push join
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

        attr_accessor :sel_expression, :sel_group_by, :sel_unions, :gen_index_hints, 
                      :sel_distinction, :sel_having, :sel_group_by_order, :gen_where, :gen_from,
                      :gen_first, :gen_skip, :gen_order_by, :gen_order_by_order, :gen_joins

        METHODS = {
            :where    => QAttr.new( :name => 'gen_where', :text => 'WHERE', 
                                        :val => SQLConditional ),
            :from     => QAttr.new( :name => 'gen_from',  :text => 'FROM',  :val => SQLObject ),
            :all         => QAttr.new( :name => 'sel_distinction', :text => 'ALL'         ),
            :distinct    => QAttr.new( :name => 'sel_distinction', :text => 'DISTINCT'    ),
            :distinctrow => QAttr.new( :name => 'sel_distinction', :text => 'DISTINCTROW' ),
            :having      => QAttr.new( :name => 'sel_having', :text => 'HAVING', 
                                       :val  => SQLConditional ),
            :group_by    => QAttr.new( :name => 'sel_group_by', :text => 'GROUP BY',  
                                       :val => SQLObject ),
            :group_by_asc   => QAttr.new( :name => 'sel_group_by_order', :text => 'ASC'  ),
            :group_by_desc  => QAttr.new( :name => 'sel_group_by_order', :text => 'DESC' ),
            :union          => QAttr.new( 
                                :name     => 'sel_unions',   
                                :text     => 'UNION',
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :union_all      => QAttr.new(
                                :name     => 'sel_unions',
                                :text     => 'UNION_ALL',
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :union_distinct => QAttr.new(
                                :name     => 'sel_unions',   
                                :text     => 'UNION_DISTINCT', 
                                :val_type => 'list',
                                :no_commas => true,
                                :val      => SQLConstructor::BasicUnion
                               ),
            :join => SQLConstructor::QAttr.new( :name => "gen_joins", :text => "JOIN", 
                                                :val => SQLConstructor::BasicJoin, 
                                                :val_type => 'list' ),
            :first    => QAttr.new( :name => 'gen_first', :text => 'FIRST', :val => SQLObject ),
            :skip     => QAttr.new( :name => 'gen_skip',  :text => 'SKIP',  :val => SQLObject ),
            :order_by => QAttr.new( :name => 'gen_order_by', :text => 'ORDER BY', 
                                    :val => SQLObject ),
            :order_by_asc  => QAttr.new( :name => 'gen_order_by_order', :text => 'ASC' ),
            :order_by_desc => QAttr.new( :name => 'gen_order_by_order', :text => 'DESC' )
        }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @sel_expression = QAttr.new(
                                :name => 'sel_expression',
                                :text => '',
                                :val  => SQLAliasedList.new( *list )
                              )
        end

        ##########################################################################
        #   Add more objects to SELECT expression list ( @sel_expression[:val] )
        ##########################################################################
        def select_more ( *list )
            @sel_expression.val.push *list
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic DELETE statement.
  ###############################################################################################
    class BasicDelete < GenericQuery

        attr_accessor :del_using, :gen_where, :gen_from, :gen_skip, :gen_first, :gen_order_by,
                      :gen_order_by_order

        METHODS = { 
                    :using => QAttr.new( :name => 'del_using', :text => 'USING', 
                                         :val => SQLObject ),
                    :join => SQLConstructor::QAttr.new( :name => "gen_joins", :text => "JOIN", 
                                                        :val => SQLConstructor::BasicJoin, 
                                                        :val_type => 'list' ),
                    :where => QAttr.new( :name => 'gen_where', :text => 'WHERE', 
                                         :val => SQLConditional ),
                    :from  => QAttr.new( :name => 'gen_from',  :text => 'FROM',  :val => SQLObject ),
                    :first => QAttr.new( :name => 'gen_first', :text => 'FIRST', :val => SQLObject ),
                    :skip  => QAttr.new( :name => 'gen_skip',  :text => 'SKIP',  :val => SQLObject ),
                    :order_by => QAttr.new( :name => 'gen_order_by', :text => 'ORDER BY', 
                                            :val => SQLObject ),
                    :order_by_asc  => QAttr.new( :name => 'gen_order_by_order', :text => 'ASC' ),
                    :order_by_desc => QAttr.new( :name => 'gen_order_by_order', :text => 'DESC' )
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
                  :columns => QAttr.new( :name => 'ins_columns', :text => 'COLUMNS', 
                                         :val => SQLObject ),
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

        attr_accessor :upd_tables, :upd_set, :gen_where, :gen_order_by, :gen_first, :gen_skip

        METHODS = {
                    :tables => QAttr.new( :name => 'upd_tables', :text => '', :val => SQLObject ),
                    :set    => QAttr.new( :name => 'upd_set', :text => 'SET', :val => SQLCondList ),
                    :where  => QAttr.new( :name => 'gen_where', :text => 'WHERE', 
                                          :val => SQLConditional ),
                    :first  => QAttr.new( :name => 'gen_first', :text => 'FIRST', :val => SQLObject ),
                    :skip   => QAttr.new( :name => 'gen_skip',  :text => 'SKIP',  :val => SQLObject ),
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

