
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
    #   Pass all unknown methods to @obj 
    ##########################################################################
    def method_missing ( method, *args )
        return @obj.send( method, *args )  if @obj
        raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + method.to_s
    end
    
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

        attr_reader :type, :dialect, :exporter, :caller, :tidy, 
                    :gen_where, :gen_from, :gen_index_hints, :gen_first, :gen_skip, 
                    :gen_order_by, :gen_joins

        METHODS = {
                :from     => { :attr => 'gen_from',  :name => 'FROM',  :val => SQLObject       },
                :where    => { :attr => 'gen_where', :name => 'WHERE', :val => SQLConditional  },
                :first    => { :attr => 'gen_first', :name => 'FIRST', :val => SQLObject       },
                :skip     => { :attr => 'gen_skip',  :name => 'SKIP',  :val => SQLObject       },
                :order_by => { :attr => 'gen_order_by', :name => 'ORDER BY', :val => SQLObject },
             }
 
        ##########################################################################
        #   Class constructor. 
        #   _caller     - the caller object
        #   *list       - list of sources for the FROM clause
        ##########################################################################
        def initialize ( _caller )
            @type     = nil
            @methods  = nil
            @string   = nil
            @caller   = _caller
            @dialect  = @caller.dialect
            @tidy     = @caller.tidy
            @exporter = _caller.exporter
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

        #############################################################################
        #   Process method calls for entities described in METHODS constant array
        #   of the calling object's class.
        #   If no corresponding entries are found in all object's parent classes, then
        #   send missing methods calls to the @caller object.
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
                method_hash = methods[method].dup
                attr_name = method_hash[:attr]
                val_obj = nil

                 # get the current value of the objects attribute {attr_name}
                begin
                    cur_attr_val = self.send( attr_name.to_sym )[:val]
                rescue
                    cur_attr_val = nil
                end
 
                 # Create an SQLList out of arg if [:val] is SQLList class:
                if method_hash[:val] == SQLList
                    method_hash[:val] = SQLList.new args
                 # Create an array of SQLObjects if [:val] is SQLObject class:
                elsif method_hash[:val] == SQLObject
                    method_hash[:val] = args.map{ |arg|  SQLObject.get arg }
                 # Create an SQLList out of arg if [:val] is SQLList class:
                elsif method_hash[:val] == SQLConstructor
                    val_obj = SQLConstructor.new(
                                                    :dialect  => @dialect,
                                                    :tidy     => @tidy,
                                                    :exporter => @exporter 
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
                elsif method_hash[:val].is_a? Class
                    val_obj = cur_attr_val || method_hash[:val].new( self )
                    method_hash[:val] = val_obj
                end

                method_hash.delete(:attr)

                 # If the object already has attribute {attr_name} defined and it's
                 # an array, then we should rather append to it than reassign the value
                if cur_attr_val.is_a? Array
                    cur_attr_val << method_hash[:val]
                    method_hash[:val] = cur_attr_val
                end

                self.class.send :attr_accessor, attr_name.to_sym  if ! cur_attr_val
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
            @string = @exporter.export self
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
            join = BasicJoin.new( self, type, *tables )
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
                raise NoMethodError, SQLException::INVALID_INDEX_HINT + ": " + type
            end
            @gen_index_hints ||= [ ]
             # set the gen_index_hints for the last object in for_vals
            last_ind = for_vals.length - 1
            @gen_index_hints[last_ind] = { :type => type, :list => SQLObject.get( list ) }
            @string = nil
            return self
        end
       
    end


  ###############################################################################################
  #   Internal class which represents a basic SELECT statement.
  ###############################################################################################
    class BasicSelect < GenericQuery

        attr_reader :sel_expression, :sel_group_by, :sel_unions, :gen_index_hints, 
                    :sel_distinction, :sel_having

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
            :union     => { :attr => 'sel_unions',   :name => 'UNION',     :val => SQLConstructor },
            :union_all => { :attr => 'sel_unions',   :name => 'UNION_ALL', :val => SQLConstructor },
            :union_distinct => { 
                                :attr => 'sel_unions',   
                                :name => 'UNION_DISTINCT', 
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
                                :val  => list.map{ |obj|  SQLObject.get obj }
                              }
        end

        #############################################################################
        #   Send missing methods calls to the @caller object, and also handle
        #   JOINs, UNIONs and INDEX hints
        #############################################################################
        def method_missing ( method, *args )
             # Handle all [*_]join calls:
            return _addJoin( method, *args )        if method =~ /^[a-z_]*join$/
             # Handle all *_index calls:
            return _addIndexes( method, @gen_from[:val], *args )  if method =~ /^[a-z]+_index$/
            super
        end

    end


  ###############################################################################################
  #   Internal class which represents a basic JOIN statement.
  ###############################################################################################
    class BasicJoin < GenericQuery

        attr_reader :join_on, :join_sources, :join_using

        VALID_JOINS = [ "JOIN", "INNER JOIN", "CROSS JOIN", "LEFT JOIN", "RIGHT JOIN", 
                        "LEFT OUTER JOIN", "RIGHT OUTER_JOIN",
                        "NATURAL JOIN JOIN", "NATURAL LEFT JOIN", "NATURAL RIGHT JOIN", 
                        "NATURAL LEFT OUTER JOIN", "NATURAL RIGHT OUTER JOIN" ]
        METHODS = {
            :on    => { :attr => 'join_on',    :name => 'ON',    :val => SQLConditional },
            :using => { :attr => 'join_using', :name => 'USING', :val => SQLObject      },
        }
 
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
            @type         = type
            @join_sources = Helper.getSources sources
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

        attr_reader :del_using

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
 
