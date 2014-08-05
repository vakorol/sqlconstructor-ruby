
###############################################################################################
###   Main class for all objects. All other entities should inherit this class.
###############################################################################################
class SQLObject

    attr_accessor :alias

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
                SQLList.new *expr.to_a
            elsif expr =~ /^\:/
                SQLColumn.new( expr )
            else
                SQLValue.new( expr )
            end
        end

         # Return array or scalar, depending on the number of function arguments
        list.length == 1  ?  list[0]  :  list
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
        @name
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
class SQLList < SQLObject
    def initialize ( *list )
        @list = list.map { |item|  SQLObject.get item }
    end

    def << ( list )
        list.map! { |item|  SQLObject.get item }
        @list << list
    end

    def to_s
        "(" + @list.join( "," ) + ")"
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
 
