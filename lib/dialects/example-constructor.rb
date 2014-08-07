


class SQLConstructor::BasicSelect_example < SQLConstructor::BasicSelect

    # attr_reader :attr_foo
 

    ##########################################################################
    #   Class constructor. 
    #   Init any attributes here. Do not forget to call 'super'!
    #   _caller     - the caller object
    #   *list       - list of sources for the FROM clause
    ##########################################################################
    def initialize ( _caller, *list )
        super
        # @attr_foo = false
    end


    ##########################################################################
    #   Define your methods here.
    #   If your dialect supports a keyword FOO, so that you would want to
    #   construct something like
    #       SELECT a FROM t WHERE b=1 FOO 5
    #   you'll need to implement a foo() method here and further use it like
    #       SQLConstructor.new.select(:a).from('t').where.eq(:b,1).foo(5)
    ##########################################################################

    # def foo ( value = nil )
    #     @attr_foo = value
    #     return self
    # end


    #############################################################################
    #   You may do mostly anything here, but don't forget to call method_missing
    #   of the parent ( 'return super' )
    #############################################################################
    def method_missing ( method, *args )
        return super
    end

end

