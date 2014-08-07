
###############################################################################################
###   Main class for all objects. All other entities should inherit this class.
###############################################################################################
class SQLObject

    attr_accessor :alias, :separator

    def initialize
        @string = nil
        @alias  = nil
    end


    def to_s
        @string = self.to_s
    end

    ##########################################################################
    #   Convert values to the corresponding internal data types
    ##########################################################################
    def self.get ( *list )
        list.map! do |expr|
            if expr.is_a? SQLObject
                expr
            elsif expr.is_a? Array or expr.is_a? Range
                SQLValList.new *expr.to_a
            elsif expr.is_a? Hash
                SQLAliasedList.new expr
            elsif expr.is_a? Symbol
                SQLColumn.new( expr )
            else
                SQLValue.new( expr )
            end
        end

         # Return array or scalar, depending on the number of function arguments
        list.length == 1  ?  list[0]  :  list
    end

    ##########################################################################
    #   Convert a hash of { obj => alias, ... } to an array of SQLObjects
    #   with defined @alias attribute.
    ##########################################################################
    def self.getWithAliases( *list )
        new_list = [ ]
         # If list is a hash of objects with aliases:
        if list.length == 1 && list[0].is_a?( Hash )
            list.each do |src, _alias|
                obj = SQLObject.get src
                obj.alias = _alias
                new_list << obj
            end
         # If list is an array of objects:
        else
            new_list = list.map { |src|  SQLObject.get src }
        end
        return new_list
    end 

end


###############################################################################################
###   Class representing SQL columns
###############################################################################################
class SQLColumn < SQLObject

    def initialize ( col = nil )
        @name = col.is_a?( SQLColumn )  ?  col.name  :  _prepareName( col )
    end

    ##########################################################################
    def to_s
        @name.to_s
    end

  private

    ##########################################################################
    #   Prepare column name (remove all non-alphanumeric/underscore characters)
    ##########################################################################
    def _prepareName ( name )
        name.to_s.gsub /[\W]/, ''
    end
  
end


###############################################################################################
###   Class representing SQL scalar values
###############################################################################################
class SQLValue < SQLObject

    def initialize ( val = nil )
        @value = val.is_a?( SQLValue )  ?  val.value  :  _escape( val )
    end

    ##########################################################################
    def to_s
        @value
    end
 
  private

    ##########################################################################
    # DERIVED FROM https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/abstract/quoting.rb
    ##########################################################################
    def _escape ( val )
        case val
            when String     then "'#{_quoteString(val.to_s)}'"
            when true       then 'TRUE'
            when false      then 'FALSE'
            when nil        then "NULL"
            when Numeric, Time then val.to_s
            when Symbol     then "'#{_quoteString(val.to_s)}'"
            when Class      then "'#{val.to_s}'"
            else
                "'#{quoteString( val.to_s )}'"
        end
    end

    ##########################################################################
    # FROM https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/abstract/quoting.rb
    # Quotes a string, escaping any ' (single quote) and \ (backslash)
    # characters.
    ##########################################################################
    def _quoteString ( str )
        str.gsub( /\\/, '\&\&' ).gsub( /'/, "''" ) # ' (for ruby-mode)
    end

end


###############################################################################################
###   Class container - a list of SQLValue scalars
###############################################################################################
class SQLValList < SQLObject
    def initialize ( *list )
        @list = list.map { |item|  SQLObject.get item }
    end

    def << ( list )
        list.map! { |item|  SQLObject.get item }
        @list += list
    end

    def to_s
        return @string  if @string
        @string = "(" + @list.join( "," ) + ")"
    end
end


###############################################################################################
###   Class container - a list of SQLObjects with aliases
###############################################################################################
class SQLAliasedList < SQLObject
    def initialize ( *list )
        @list = _getList *list
    end

    def << ( *list )
        @list += _getList *list
        return self 
    end
 
    def to_s
        return @string  if @string
        arr = @list.map { |obj|  obj.to_s + ( obj.alias  ? " " + obj.alias.to_s  : "" ) }
        @string = arr.join ","
    end

  private

    def _getList ( *list )
        new_list = [ ]
         # If list is a hash of objects with aliases:
        if list.length == 1 && list[0].is_a?( Hash )
            new_list += _hash2array list[0]
         # If list is an array of objects:
        else
            new_list = list.map { |src|  SQLObject.get src }
        end
    end

    def _hash2array ( hash )
        list = [ ]
        hash.each do |item, _alias|
            obj = SQLObject.get item
            obj.alias = _alias
            list << obj
        end
        return list
    end

end
  

###############################################################################################
###
###############################################################################################
class SQLCondList < SQLObject
    def initialize ( hash = { } )
        @hash = Hash[ hash.map{ |k,v|  [ SQLObject.get( k ), SQLObject.get( v ) ] } ]
    end

    def << ( new_hash )
        @hash.merge! new_hash
    end

    def to_s
        @hash.map{ |k,v| k.to_s + "=" + v.to_s }.join( "," )
    end
end
 
