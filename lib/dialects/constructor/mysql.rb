
class SQLConstructor::BasicSelect_mysql < SQLConstructor::BasicSelect

    attr_reader :attr_high_priority, :attr_straight_join, 
                :attr_sql_result_size, :attr_sql_cache, :attr_sql_calc_found_rows

    VALID_INDEX_HINTS = [ "USE INDEX", "FORCE INDEX", "IGNORE INDEX" ]
    VALID_SQL_RESULT_SIZES = %w/SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT/


    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    #   *list       - list of sources for the FROM clause
    ##########################################################################
    def initialize ( _caller, *list )
        super
        @attr_high_priority       = nil
        @attr_straight_join       = nil
        @attr_sql_result_size     = nil
        @attr_sql_cache           = nil
        @attr_sql_calc_found_rows = nil
    end


    def high_priority
        @attr_high_priority = "HIGH_PRIORITY";
        @string = nil
        return self
    end


    ##########################################################################
    #   Set the value for the SKIP,FIRST or LIMIT statement. Must be numeric value.
    ##########################################################################
    def limit ( val1, val2 = nil )
        if val2
            self.first val2
            self.skip  val1
        else
            self.first val1
        end
    end    


    def straight_join
        @attr_straight_join = "STRAIGHT_JOIN";
        @string = nil
        return self
    end


    def sql_cache
        @attr_sql_cache = "SQL_CACHE";
        @string = nil
        return self
    end
 
    def sql_no_cache
        @attr_sql_cache = "SQL_NO_CACHE";
        @string = nil
        return self
    end


    def sql_calc_found_rows
        @attr_sql_calc_found_rows = "SQL_CALC_FOUND_ROWS";
        @string = nil
        return self
    end
  

    #############################################################################
    #   Handle sql result size directives or pass method to parent
    #############################################################################
    def method_missing ( method, *args )
        return _addSQLResultSize( method, *args )  if method =~ /^SQL_(?:SMALL|BIG|BUFFER)_RESULT$/
         # Send missing method calls to @caller
        return super
    end


  protected

    ##########################################################################
    #   Adds an SQL_[SMALL|BIG|BUFFER]_RESULT directive
    ##########################################################################
    def _addSQLResultSize ( type )
        type = type.to_s.upcase
        if ! VALID_SQL_RESULT_SIZES.include? type
            raise NoMethodError, SQLException::UNKNOWN_METHOD + ": " + type
        end
        @attr_sql_result_size = type
        @string = nil
        return obj
    end
    
end


class SQLConstructor::BasicDelete_mysql < SQLConstructor::BasicDelete

    attr_reader :attr_ignore, :attr_quick, :attr_low_priority

    METHODS = {
                :low_priority => { :attr => 'del_priority', :name => 'LOW_PRIORITY' },
                :quick        => { :attr => 'del_quick',    :name => 'QUICK '       },
                :ignore       => { :attr => 'del_ignore',   :name => 'IGNORE'       }
              }
 
    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    ##########################################################################
    def initialize ( _caller )
        super
        @attr_ignore = nil
        @attr_low_priority = nil
        @attr_quick = nil
    end

    ##########################################################################
    #   Set the value for the SKIP,FIRST or LIMIT statement. Must be numeric value.
    ##########################################################################
    def limit ( val1, val2 = nil )
        if val2
            self.first val2
            self.skip  val1
        else
            self.first val1
        end
    end    

end 


class SQLConstructor::BasicInsert_mysql < SQLConstructor::BasicInsert

    attr_reader :ins_priority, :ins_quick, :ins_ignore, :ins_on_duplicate_key_update

    METHODS = {
                :low_priority  => { :attr => 'ins_priority', :name => 'LOW_PRIORITY'  },
                :delayed       => { :attr => 'ins_priority', :name => 'DELAYED'       },
                :high_priority => { :attr => 'ins_priority', :name => 'HIGH_PRIORITY' },
                :quick         => { :attr => 'ins_quick',    :name => 'QUICK'         },
                :ignore        => { :attr => 'ins_ignore',   :name => 'IGNORE'        },
                :on_duplicate_key_update => { 
                                              :attr => 'ins_on_duplicate_key_update', 
                                              :name => 'ON DUPLICATE KEY UPDATE',
                                              :val  => SQLConditional,
                                            }
              }

    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    ##########################################################################
    def initialize ( _caller )
        super
    end

end 
