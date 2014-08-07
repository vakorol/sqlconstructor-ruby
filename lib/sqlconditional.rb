
##################################################################################################
#   This class represents an SQL conditional statement.
#
#   Author::    Vasiliy Korol  (mailto:vakorol@mail.ru)
#   Copyright:: Vasiliy Korol (c) 2014
#   License::   Distributes under terms of GPLv2
##################################################################################################
class SQLConditional < SQLObject

    attr_accessor :caller

     # Dirty hack to make .join work on an array of SQLObjects
    alias :to_str :to_s
 
    ##########################################################################
    #   Class constructor. Accepts an optional parameter to set the @caller 
    #   attribute, which is used in method_missing magic method to return 
    #   control to the calling SQLConstructor object (see method_missing for 
    #   more info).
    ##########################################################################
    def initialize ( params = nil )
        @dialect, @tidy, @separator = nil, false, " "
        if params.is_a? Hash
            @caller = params[ :caller  ]
            if @caller
                @dialect   = params[ :dialect ] || @caller.dialect
                @tidy      = params[ :tidy    ] || @caller.tidy
                @separator = @caller.exporter.separator  if @caller.exporter  
            end
        end
        @list    = [ ]
        @objects = [ ]
        @string  = nil
    end

    ##########################################################################
    #   Adds another SQLConditional object to the conditions list of the
    #   current object. Example: 
    #       <tt>cond1 = SQLConditional.new.eq(':c1',3)</tt>
    #       <tt>cond2 = SQLConditional.new.lt(':c2',5).and.is(cond2)</tt>
    ##########################################################################
    def is ( cond )
        raise SQLException, ERR_INVALID_CONDITIONAL  if ! cond.is_a? SQLObject
        cond.separator = @separator
        @list << cond
        @string = nil
        return self     
    end

    ##########################################################################
    #    Negates the following conditional statement.
    ##########################################################################
    def not ( expr )
        _addBasicCond( :NOT, BasicCond::LHS )
    end


    def and
        _addBasicCond( :AND, BasicCond::MIDDLE )
    end


    def or
        _addBasicCond( :OR, BasicCond::MIDDLE )
    end


    def eq ( expr1, expr2 )
        _addBasicCond( :'=', BasicCond::MIDDLE, expr1, expr2 )
    end


    def ne ( expr1, expr2 )
        _addBasicCond( :'!=', BasicCond::MIDDLE, expr1, expr2 )
    end
 

    def gt ( expr1, expr2 )
        _addBasicCond( :'>', BasicCond::MIDDLE, expr1, expr2 )
    end


    def lt ( expr1, expr2 )
        _addBasicCond( :'<', BasicCond::MIDDLE, expr1, expr2 )
    end


    def gte ( expr1, expr2 ) 
        _addBasicCond( :'>=', BasicCond::MIDDLE, expr1, expr2 )
    end


    def lte ( expr1, expr2 ) 
        _addBasicCond( :'<=', BasicCond::MIDDLE, expr1, expr2 )
    end


    def is_null ( expr )
        _addBasicCond( :'IS NULL', BasicCond::RHS, SQLObject.get( expr ) )
    end


    def in ( expr1, expr2 ) 
        _addBasicCond( :'IN', BasicCond::MIDDLE, expr1, expr2 )
    end


    def like ( expr1, expr2 )
        _addBasicCond( :'LIKE', BasicCond::MIDDLE, expr1, expr2 )
    end


    def to_s
        return @string  if @string
        @string = @separator
        @string += "("

        @list.each do |item|
            @string += item.to_s
        end

        @string += ")"

        return @string
    end


    #############################################################################
    #   This magic method is used to return control to the SQLConstructor object
    #   stored in @caller from the SQLConditional object derived by the .where()
    #   method of the SQLConstructor class. This allows mixing the methods of the
    #   two classes, i.e.:  
    #       <tt>SQLConstructor.new.select(':a').from('tab').where.eq(':b',3).limit(5)</tt>
    #   Here .where.eq() returns an SQLConditional object, but further application
    #   of the foreign method .limit() returns back to the SQLConstructor object.
    #############################################################################
    def method_missing ( method, *args )
        return @caller.send( method.to_sym, *args )  if @caller
        raise SQLException, ERR_UNKNOWN_METHOD + ": " + method
    end


  private

    def _addBasicCond ( operator, type, *expressions )
        objects = SQLObject.get( *expressions )
        @list << BasicCond.new( operator, type, *objects )
        @string = nil
        return self
    end


  ###############################################################################################
  #   Internal class which represents a basic logical operation.
  ###############################################################################################
    class BasicCond

        LHS    = -1
        MIDDLE =  0 
        RHS    =  1

        def initialize ( operator, type, *objects )
            @operator, @type, @objects = operator.to_s, type, objects
            @string = nil
        end

        def to_s
            return @string  if @string
                @string = " "
            if @objects.empty?
                @string += " " + @operator + " "
            else
                case @type
                    when LHS 
                        @string = @operator + " " + @objects[0].to_s
                    when RHS
                        @string = @objects[0].to_s + " " + @operator
                    when MIDDLE 
                        @string = @objects.empty?  ?  " " + @operator + " " 
                                               :  @objects.join( " " + @operator + " " )
                    else
                        raise SQLException, ERR_UNKNOWN_OPERATOR_TYPE
                end
            end
        end

    end

end

