
require_relative "sqlobject"
require_relative "sqlconditional"
require_relative "sqlexporter"
require_relative "sqlerrors"

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
 
    VALID_INDEX_HINTS = [ "USE INDEX", "FORCE INDEX", "IGNORE INDEX" ]
 
    ##########################################################################
    #   Class constructor. Accepts an optional argument with a hash of
    #   parameters :dialect and :tidy to set the SQLExporter object in @exporter,
    #   or :exporter to receive a predefined SQLExporter object.
    ##########################################################################
    def initialize ( params = nil )
        @dialect, @tidy = nil, false
        if params.is_a? Hash
            @dialect  = params[ :dialect  ]
            @tidy     = params[ :tidy     ]
            @exporter = params[ :exporter ]
        end
        @exporter ||= SQLExporter.new @dialect, @tidy
        @obj     = nil
        @string  = false
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
        if @obj && @obj.child_caller != @obj
            return @obj.send( method, *args )
        end
        raise NoMethodError, ERR_UNKNOWN_METHOD + 
            ": '#{method.to_s}' from #{@obj.class.name}"
    end
    
    ##########################################################################
    #   Convert object to string by calling the .export() method of
    #   the @exporter object.
    ##########################################################################
    def to_s
        return @string  if @string
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
        class_child = class_basic +'_' + @dialect
        begin
            @obj = self.class.const_get( class_child ).new self, *args
        rescue NameError
            @obj = self.class.const_get( class_basic ).new self, *args
        end
    end
 

  ###############################################################################################
  #   Internal class - generic query attributes and methods. Should be parent to all Basic*
  #   classes
  ###############################################################################################
    class GenericQuery < SQLObject

        attr_accessor :caller
        attr_reader :type, :dialect, :exporter, :child_caller, :tidy, 
                    :gen_where, :gen_from, :gen_index_hints, :gen_first, :gen_skip, 
                    :gen_order_by, :gen_joins, :gen_order_by_order

        METHODS = {
                :from     => { :attr => 'gen_from',  :name => 'FROM',  :val => SQLObject       },
                :where    => { :attr => 'gen_where', :name => 'WHERE', :val => SQLConditional  },
                :first    => { :attr => 'gen_first', :name => 'FIRST', :val => SQLObject       },
                :skip     => { :attr => 'gen_skip',  :name => 'SKIP',  :val => SQLObject       },
                :order_by => { :attr => 'gen_order_by', :name => 'ORDER BY', :val => SQLObject },
                :order_by_asc  => { :attr => 'gen_order_by_order', :name => 'ASC' },
                :order_by_desc => { :attr => 'gen_order_by_order', :name => 'DESC' },
               }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller )
            @caller   = _caller
            @dialect  = @caller.dialect
            @tidy     = @caller.tidy
            @exporter = _caller.exporter
            begin
                @methods = self.getMethods
            rescue
                @methods = { }
            end
         end

        ##########################################################################
        #   Returns the METHODS hash of child dialect-specific class merged with
        #   parent's METHODS hash.
        ##########################################################################
        def getMethods
            if ! @methods
                methods_self = { }
                self.class.ancestors.each do |_class| 
                    next  if ! _class.ancestors.include? SQLConstructor::GenericQuery
                    class_methods = _class.const_get :METHODS || { }
                    methods_self.merge! class_methods
                end
                @methods = methods_self
            end
            return @methods
        end

        ##########################################################################
        #   Returns an object by clause (keys of METHODS hash) or by SQLObject.name
        ##########################################################################
        def _get ( clause, name = nil )
            if name && clause == :join
                _getJoinByName name
            else
                super clause
            end
        end
 
        ##########################################################################
        #   Removes attribute by method name (specified in the METHODS constant)
        #   This method must be ovverriden in child classes if any methods were 
        #   defined explicitly (not in METHODS).
        ##########################################################################
        def _remove ( clause, name = nil )
            if clause == :join
                _removeJoinByName name
            elsif @methods.has_key? clause
                method_hash = @methods[clause].dup
                attr_name = method_hash[:attr]
                self.send "#{attr_name}=", nil
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
        #   Process method calls for entities described in METHODS constant array
        #   of the calling object's class.
        #   If no corresponding entries are found in all object's parent classes, 
        #   then send missing methods calls to the @caller object.
        ##########################################################################
        def method_missing ( method, *args )
             # If the method is described in the class' METHODS constant hash, then
             # create an instance attribute with the proper name, an attr_reader for
             # it, and set it's value to the one in METHODS.
            if @methods.has_key? method
                method_hash = @methods[method].dup
                attr_name = method_hash[:attr]
                cur_attr = self.send( attr_name.to_sym )
                val_obj = nil

                 # get the current value of the objects attribute {attr_name}
                if self.respond_to?( attr_name.to_sym ) && cur_attr.is_a?( Hash )
                    cur_attr_val = cur_attr[:val]
                else
                    cur_attr_val = nil
                end
 
                 # Create an instance of the corresponding class if [:val] is SQLList 
                 # or SQLCondList class:
                if [ SQLValList, SQLAliasedList, SQLCondList ].include? method_hash[:val]
                    method_hash[:val] = method_hash[:val].new *args

                 # Create an array of SQLObjects if [:val] is SQLObject class:
                elsif method_hash[:val] == SQLObject
                    method_hash[:val] = args.map{ |arg|  SQLObject.get arg }

                 # Create an instance of the corresponding class if [:val] is 
                 # SQLConstructor or SQLConditional class:
                elsif [ SQLConstructor, SQLConditional ].include? method_hash[:val]
                    val_obj = cur_attr_val || method_hash[:val].new(
                                                    :dialect  => @dialect,
                                                    :tidy     => @tidy,
                                                    :exporter => @exporter,
                                                    :caller   => self
                                              )
                    method_hash[:val] = val_obj

                 # create a BasicSelect dialect-specific child class: 
                elsif method_hash[:val].is_a? BasicSelect
                    method_hash[:val] = SQLConstructor.new( 
                                                            :dialect  => @dialect, 
                                                            :tidy     => @tidy,
                                                            :exporter => @exporter 
                                                          ).select( *args )

                 # If the :val parameter is some different class, then we should 
                 # create an instance of it or return the existing value:
#                elsif method_hash[:val].is_a? Class
#                    val_obj = cur_attr_val || method_hash[:val].new( self )
#                    method_hash[:val] = val_obj
                end

                method_hash.delete :attr

                 # If the object already has attribute {attr_name} defined and it's
                 # an array or one of the SQL* class containers, then we should rather 
                 # append to it than reassign the value
                if [ Array, SQLValList, SQLAliasedList, SQLCondList ].include?( cur_attr_val.class )
                    cur_attr_val << method_hash[:val]
                    method_hash[:val] = cur_attr_val
                end

                self.class.send :attr_accessor, attr_name.to_sym  if ! cur_attr_val
                self.send "#{attr_name}=", method_hash

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
            @gen_joins ||= [ ]
            join = _getBasicJoin( type, *tables )
            @gen_joins << join
            return join
        end

        ##########################################################################
        #   Adds a USE/FORCE/IGNORE INDEX clause for the last objects in for_vals
        #   argument.
        ##########################################################################
        def _addIndexes ( type, for_vals, *list )
            type = type.to_s
            type.upcase!.gsub! /_/, ' '
            if ! SQLConstructor::VALID_INDEX_HINTS.include? type
                raise NoMethodError, ERR_INVALID_INDEX_HINT + ": " + type
            end
            @gen_index_hints ||= [ ]
             # set the gen_index_hints for the last object in for_vals
            last_ind = for_vals.length - 1
            @gen_index_hints[last_ind] = { :type => type, :list => SQLObject.get( list ) }
            @string = nil
            return self
        end

        ##########################################################################
        #   Returns an instance of BasicJoin_* child dialect-specific class 
        ##########################################################################
        def _getBasicJoin ( type, *tables )
            class_basic = 'BasicJoin'
            class_child = class_basic +'_' + @dialect
            begin
                @obj = SQLConstructor.const_get( class_child ).new self, type, *tables
            rescue NameError
                @obj = SQLConstructor.const_get( class_basic ).new self, type, *tables
            end
        end

        ##########################################################################
        #   Returns a named BasicJoin* object by name from @sel_joins array
        ##########################################################################
        def _getJoinByName ( name )
            return nil  if ! @gen_joins || ! name
            @gen_joins.each { |join|  return join  if join.name == name }
            return nil
        end

        ##########################################################################
        #   Removes a named BasicJoin* object by name from @sel_joins array
        ##########################################################################
        def _removeJoinByName ( name )
            return self  if ! @gen_joins || ! name
            @gen_joins.delete_if { |join|  join.name == name }
            return self
        end
  
    end


  ###############################################################################################
  #   Internal class which represents a basic SELECT statement.
  ###############################################################################################
    class BasicSelect < GenericQuery

        attr_accessor :sel_expression, :sel_group_by, :sel_unions, :gen_index_hints, 
                    :sel_distinction, :sel_having, :sel_group_by_order 

        METHODS = {
            :all         => { :attr => 'sel_distinction', :name => 'ALL'      },
            :distinct    => { :attr => 'sel_distinction', :name => 'DISTINCT' },
            :distinctrow => { :attr => 'sel_distinction', :name => 'DISTINCTROW' },
            :having      => { 
                                :attr => 'sel_having', 
                                :name => 'HAVING', 
                                :val  => SQLConditional       
                            },
            :group_by  => { :attr => 'sel_group_by', :name => 'GROUP BY',  :val => SQLObject },
            :group_by_asc   => { :attr => 'sel_group_by_order', :name => 'ASC' },
            :group_by_desc  => { :attr => 'sel_group_by_order', :name => 'DESC' },
            :union          => { 
                                :attr => 'sel_unions',   
                                :name => 'UNION',
                                :val_type => 'list',
                                :val => SQLConstructor 
                               },
            :union_all      => { 
                                :attr => 'sel_unions',
                                :name => 'UNION_ALL',
                                :val_type => 'list',
                                :val => SQLConstructor
                               },
            :union_distinct => { 
                                :attr => 'sel_unions',   
                                :name => 'UNION_DISTINCT', 
                                :val_type => 'list',
                                :val  => SQLConstructor 
                               },
        }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @sel_expression = {
                                :attr => 'sel_expression',
                                :name => '',
                                :val  => SQLAliasedList.new( *list )
                              }
        end

        ##########################################################################
        #   Add more objects to SELECT expression list ( @sel_expression[:val] )
        ##########################################################################
        def select_more ( *list )
            @sel_expression[:val].push *list
        end

        ##########################################################################
        #   Returns an object by clause (keys of METHODS hash) or by SQLObject.name
        ##########################################################################
        def _get ( clause, name = nil )
            if name && clause == :union
                _getUnionByName name
            else
                super clause
            end
        end

        ##########################################################################
        #   Deletes an object by clause (keys of METHODS hash) or by SQLObject.name
        ##########################################################################
        def _remove ( clause, name = nil )
            if name && clause == :union
                _removeUnionByName name
            else
                super clause
            end
        end

        ##########################################################################
        #   Send missing methods calls to the @caller object, and also handle
        #   JOINs, UNIONs and INDEX hints
        ##########################################################################
        def method_missing ( method, *args )
             # Handle all [*_]join calls:
            return _addJoin( method, *args )  if method =~ /^[a-z_]*join$/
              # Handle all *_index calls:
            return _addIndexes( method, @gen_from[:val], *args )  if method =~ /^[a-z]+_index$/
            super
        end

      #########
      private
      #########

        ##########################################################################
        #   Returns a named SQLConstructor object by name from @sel_unions array
        ##########################################################################
        def _getUnionByName ( name )
            return nil  if ! @sel_unions || ! name
            @sel_unions.each { |union|  return union  if union.name == name }
            return nil
        end

        ##########################################################################
        #   Removes a named SQLConstructor object by name from @sel_unions array
        ##########################################################################
        def _removeUnionByName ( name )
            return self  if ! @sel_unions || ! name
            @sel_unions.delete_if { |union|  union[:val].name == name }
            return self
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic JOIN statement.
  ###############################################################################################
    class BasicJoin < GenericQuery

        attr_accessor :join_on, :join_sources, :join_using

        METHODS = {
            :on    => { :attr => 'join_on',    :name => 'ON',    :val => SQLConditional },
            :using => { :attr => 'join_using', :name => 'USING', :val => SQLObject      },
        }
 
        #############################################################################
        #   Class contructor. Takes a caller object as the first argument, JOIN 
        #   type as the second argument, and a list of sources for the JOIN clause
        #############################################################################
        def initialize ( _caller, type, *sources )
            type = type.to_s
            type.upcase!.gsub! /_/, ' '
  
            super _caller
            @type         = type
            @join_sources = SQLAliasedList.new *sources
        end

        def join_more ( *sources )
            @join_sources.push *sources
        end

        #############################################################################
        #   Returns control to the SQLConstructor object stored in @caller and
        #   handles INDEX hints.
        #############################################################################
        def method_missing ( method, *args )
             # Handle all *_index calls:
            return _addIndexes( method, @join_sources, *args )  if method =~ /^[a-z]+_index$/
            super
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic DELETE statement.
  ###############################################################################################
    class BasicDelete < GenericQuery

        attr_accessor :del_using

        METHODS = {
                    :using   => { :attr => 'del_using', :name => 'USING', :val => SQLObject }
                  }

        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            super
        end

        #############################################################################
        #   Handle JOINs or send call to the parent.
        #############################################################################
        def method_missing ( method, *args )
             # Handle all [*_]join calls:
            return _addJoin( method, *args )  if method =~ /^[a-z_]*join$/
            super
        end
 
    end 


  ###############################################################################################
  #   Internal class which represents a basic INSERT statement.
  ###############################################################################################
    class BasicInsert < GenericQuery

        attr_reader :ins_into, :ins_values, :ins_set, :ins_columns, :ins_select

        METHODS = {
                    :into    => { :attr => 'ins_into',    :name => 'INTO',    :val => SQLObject },
                    :values  => { :attr => 'ins_values',  :name => 'VALUES',  :val => SQLValList   },
                    :set     => { :attr => 'ins_set',     :name => 'SET',     :val => SQLCondList },
                    :columns => { :attr => 'ins_columns', :name => 'COLUMNS', :val => SQLObject },
                    :select  => { :attr => 'ins_select',  :name => '',        :val => BasicSelect }
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

        attr_accessor :upd_tables, :upd_set, :gen_where, :gen_order_by

        METHODS = {
                    :tables  => { :attr => 'upd_tables',  :name => '', :val => SQLObject },
                    :set     => { :attr => 'upd_set',  :name => 'SET', :val => SQLCondList },
                  }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @upd_tables = {
                              :attr => 'upd_tables',
                              :name => '',
                              :val  => SQLAliasedList.new( *list )
                          }
        end

        ##########################################################################
        #   Add tables to UPDATE tables list ( @upd_tables[:val] )
        ##########################################################################
        def update_more ( *list )
            @upd_tables[:val].push *list
        end

    end 
 
end
 

##################################################################################################
##################################################################################################
#   Include dialect-specific classes from ./dialects/constructor/  :
#   This should be done after SQLConstructor is defined.
##################################################################################################
##################################################################################################

Dir["./dialects/*-constructor.rb"].each { |file| require file }
 
