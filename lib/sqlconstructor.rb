
require_relative "sqlobject"
require_relative "helper"
require_relative "sqlconditional"
require_relative "sqlexporter"
require_relative "sqlexception"

##################################################################################################
#   This class implements methods to construct a valid SQL query.
#
#   Author::    Vasiliy Korol  (mailto:vakorol@mail.ru)
#   Copyright:: Vasiliy Korol (c) 2014
#   License::   Distributes under terms of GPLv2
##################################################################################################
class SQLConstructor < SQLObject

    attr_accessor :exporter, :tidy
    attr_reader   :obj, :current_alias, :dialect

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
    #   Pass all unknown methods to @obj 
    ##########################################################################
    def method_missing ( method, *args )
        return @obj.send( method, *args )  if @obj
        raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + method.to_s
    end
    
    ##########################################################################
    def to_s
        return @string  if @string
        @string = @exporter.print @obj
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

        attr_reader :type, :dialect, :exporter, :caller, :tidy, :attr_where, :attr_from, 
                    :attr_index_hints, :attr_first, :attr_skip, :attr_order_by, :attr_joins

        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller )
            @attr_from        = nil
            @attr_where       = nil
            @attr_index_hints = nil
            @attr_joins       = nil
            @type             = nil
            @string           = nil
            @caller           = _caller
            @dialect          = @caller.dialect
            @tidy             = @caller.tidy
            @exporter         = _caller.exporter
        end


        ##########################################################################
        #   Returns the METHODS hash of child dialect-specific class merged with
        #   parent's METHODS hash.
        ##########################################################################
        def getMethods
            ancestor = self.class.ancestors[1]
            methods_self    = self.class.const_get :METHODS || { }
            methods_ancestor  = ancestor.const_get :METHODS || { }
            methods_ancestor.merge methods_self
        end


        ##########################################################################
        #   Adds a list of table names to the FROM clause
        ##########################################################################
        def from ( *sources )
            new_list = Helper.getSources *sources
            @attr_from ||= [ ]
            @attr_from += new_list
            @string = nil
            return self
        end

        ##########################################################################
        #   Creates a new SQLConditional object for the WHERE clause.
        ##########################################################################
        def where
            @string = nil
            @attr_where ||= SQLConditional.new self
        end

        ##########################################################################
        #   Set the value for the SKIP (or LIMIT) statement. Must be numeric value.
        ##########################################################################
        def skip ( num )
            raise( TypeError, SQLException::NUMERIC_VALUE_EXPECTED )  if ! num.is_a? Numeric
            @attr_skip = SQLObject.get num
            @string   = nil
            return self
        end

        ##########################################################################
        #   Set the value for the FIRST (or LIMIT) statement. Must be numeric value.
        ##########################################################################
        def first ( num )
            raise( TypeError, SQLException::NUMERIC_VALUE_EXPECTED )  if ! num.is_a? Numeric
            @attr_first  = SQLObject.get num
            @string = nil
            return self
        end

        ##########################################################################
        #   Adds a list of ORDER BY items.
        ##########################################################################
        def order_by ( *list )
            @attr_order_by ||= [ ]
            @attr_order_by.append list.map { |item|  SQLObject.get item }
            @string = nil
            return self
        end

        #############################################################################
        #   Send missing methods calls to the @caller object
        #############################################################################
        def method_missing ( method, *args )
             # If the method is described in the class' METHODS constant hash, then
             # create an instance attribute with the proper name, an attr_reader for
             # it, and set it's value to the one in METHODS.
            begin
                methods = self.getMethods
            rescue
                methods = nil
            end

            if methods.has_key? method
                method_hash = methods[method]
                attr_name = method_hash[:attr]
                val_obj = nil

                 # Create an SQLList out of arg if [:val] is SQLList class
                if method_hash[:val] == SQLList
                    method_hash[:val] = SQLList.new args
                 # Create an array of SQLObjects if [:val] is SQLObject class
                elsif method_hash[:val] == SQLObject
                    method_hash[:val] = args.map{ |arg|  SQLObject.get arg }
                 # create an  sqlobjects if [:val] is sqlobject class
                elsif method_hash[:val].is_a? BasicSelect
                    method_hash[:val] = SQLConstructor.new( 
                                                            :dialect => @dialect, 
                                                            :tidy => @tidy,
                                                            :exporter => @exporter 
                                                          ).select( *args )
                 # If the :val parameter is some different class, then we should 
                 # create an instance of it:
                elsif method_hash[:val].is_a? Class
                    val_obj = method_hash[:val].new self
                    method_hash[:val] = val.obj
                end

                method_hash.delete(:attr)
                self.class.send :attr_accessor, attr_name.to_sym
                self.send "#{attr_name}=", method_hash

                @string = nil
                return ( val_obj || self )
            end

             # Otherwise send the call to @caller object
            return @caller.send( method.to_sym, *args )  if @caller
            raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + method.to_s
        end

        ##########################################################################
        def to_s
            return @string  if @string
            @string = @exporter.print( self )
        end

     ###########
      protected
     ###########

        ##########################################################################
        #   Creates a new BasicJoin object for the JOIN statement.
        ##########################################################################
        def _addJoin ( type, *tables )
            @string = nil
            @attr_joins ||= [ ]
            join = BasicJoin.new( self, type, *tables )
            @attr_joins << join
            return join
        end
       
    end


    ###############################################################################################
    #   Internal class which represents a basic SELECT statement.
    ###############################################################################################
    class BasicSelect < GenericQuery

        attr_reader :attr_objects, 
                    :attr_group_by, :attr_union, :attr_index_hints, 
                    :attr_distinction, :attr_having

        VALID_UNIONS = %w/union union_all union_distinct/

        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller, *list )
            super _caller
            @attr_first               = nil
            @attr_skip                = nil 
            @attr_order_by            = nil
            @attr_group_by            = nil
            @attr_union               = nil
            @attr_joins               = nil
            @attr_distinction         = nil
            @attr_having              = nil
            @attr_objects = list.map{ |obj|  SQLObject.get obj } 
        end


        def all
            @attr_distinction = "ALL"
            @string = nil
            return self
        end 
 
        def distinct
            @attr_distinction = "DISTINCT"
            @string = nil
            return self
        end 

        def distinctrow
            @attr_distinction = "DISTINCTROW"
            @string = nil
            return self
        end 
 
        ##########################################################################
        #   Creates a new SQLConditional object for the HAVING clause.
        ##########################################################################
        def having
            @string = nil
            @attr_having ||= SQLConditional.new self
        end

        ##########################################################################
        #   Adds a list of GROUP BY items.
        ##########################################################################
        def group_by ( *list )
            @attr_group_by ||= [ ]
            @attr_group_by.append list.map { |item|  SQLObject.get item }
            @string = nil
            return self
        end
 
        #############################################################################
        #   Send missing methods calls to the @caller object, and also handle
        #   JOINs, UNIONs and INDEX hints
        #############################################################################
        def method_missing ( method, *args )
             # Handle all [*_]join calls:
            return _addJoin( method, *args )        if method =~ /^[a-z_]*join$/
             # Handle all *_index calls:
            return _addIndexes( method, *args )     if method =~ /^[a-z]+_index$/
             # Handle all union[_*] calls:
            return _addUnion( method, *args )       if method =~ /^union(?:_[a-z]+)?$/
            super
        end

     #########
      protected
     #########

        ##########################################################################
        #   Adds a USE/FORCE/IGNORE INDEX clause for the last object in the
        #   FROM clause.
        ##########################################################################
        def _addIndexes ( type, *list )
            if ! SQLConstructor::VALID_INDEX_HINTS.include? type
                raise NoMethodError, SQLException::INVALID_INDEX_HINT + ": " + type
            end
            @attr_index_hints ||= [ ]
             # set the indexes for the last object in @attr_from
            from_index = @attr_from.length - 1
            @attr_index_hints[from_index] = { :type => type, :list => SQLObject.get( list ) }
            @string = nil
            return self
        end

        ##########################################################################
        #   Create a new BasicSelect object and set the @union attribute value of self 
        #   to a hash containing object and specified type
        ##########################################################################
        def _addUnion ( type )
            type = type.to_s
            if ! VALID_UNIONS.include? type
                raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + type
            end
            type.upcase!.gsub! /_/, ' '
            obj = SQLConstructor.new( :exporter => @exporter, :dialect => @dialect,
                                      :tidy => @tidy )
            @attr_union = { :object => obj, :type => type }
            @string = nil
            return obj
        end

    end


    ###############################################################################################
    #   Internal class which represents a basic JOIN statement.
    ###############################################################################################
    class BasicJoin < GenericQuery

        attr_reader :on_obj, :sources, :using_list

        VALID_JOINS = [ "JOIN", "INNER JOIN", "CROSS JOIN", "LEFT JOIN", "RIGHT JOIN", 
                        "LEFT OUTER JOIN", "RIGHT OUTER_JOIN",
                        "NATURAL JOIN JOIN", "NATURAL LEFT JOIN", "NATURAL RIGHT JOIN", 
                        "NATURAL LEFT OUTER JOIN", "NATURAL RIGHT OUTER JOIN" ]

        ##########################################################################
        #   Class contructor. Takes a caller object as the first argument, JOIN 
        #   type as the second argument, and a list of sources for the JOIN clause
        ##########################################################################
        def initialize ( _caller, type, *sources )
            type = type.to_s
            type.upcase!.gsub! /_/, ' '
            if ! BasicJoin::VALID_JOINS.include? type
                raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + type
            end
  
            super _caller
            @type       = type
            @sources    = Helper.getSources sources
            @using_list = nil
            @on_obj     = nil
        end

        #############################################################################
        #   Adds an ON condition to the JOIN statement
        #############################################################################
        def on
            @string   = nil
            @on_obj ||= SQLConditional.new( self )
        end

        #############################################################################
        #   Adds an USING condition to the JOIN statement
        #############################################################################
        def using ( *cols )
            @using_list ||= [ ]
            @using_list.append cols.map { |col|  SQLObject.get col }
            @string = nil
            return self
        end

        #############################################################################
        #   Returns control to the SQLConstructor object stored in @caller and
        #   handles INDEX hints.
        #############################################################################
        def method_missing ( method, *args )
             # Handle all *_index calls:
            return _addIndexes( method, *args )  if method =~ /^[a-z]+_index$/
            super
        end

     #########
      protected
     #########

        def _addIndexes ( type, *list )
            type = type.to_s
            type.upcase!.gsub! /_/, ' '
            if ! SQLConstructor::VALID_INDEX_HINTS.include? type
                raise NoMethodError, SQLException::INVALID_INDEX_HINT + ": " + type
            end
            @attr_index_hints ||= [ ]
             # set the attr_index_hints for the last object in @sources
            sources_index = @sources.length - 1
            @attr_index_hints[sources_index] = { :type => type, :list => SQLObject.get( list ) }
            @string = nil
            return self
        end

    end


    ###############################################################################################
    #   Internal class which represents a basic DELETE statement.
    ###############################################################################################
    class BasicDelete < GenericQuery

        attr_reader :attr_using

        METHODS = {
                    :using   => { :attr => 'del_using', :name => 'USING', :val => SQLObject }
                  }

        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            super _caller
            @attr_using = nil
        end


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
                    :values  => { :attr => 'ins_values',  :name => 'VALUES',  :val => SQLList   },
                    :set     => { :attr => 'ins_set',     :name => 'SET',     :val => SQLConditional },
                    :columns => { :attr => 'ins_columns', :name => 'COLUMNS', :val => SQLObject },
                    :select  => { :attr => 'ins_select',  :name => '',        :val => BasicSelect },
                  }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        ##########################################################################
        def initialize ( _caller )
            super
        end

    end 
 
end


##################################################################################################
##################################################################################################
#   Include dialect-specific classes from ./dialects/constructor/  :
#   This should be done after SQLConstructor is defined.
##################################################################################################
##################################################################################################

Dir["./dialects/constructor/*.rb"].each { |file| require file }
 
