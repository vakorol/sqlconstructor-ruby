
class SQLConstructor::BasicSelect_mysql < SQLConstructor::BasicSelect

    attr_reader :sel_high_priority, :sel_straight_join, :sel_sql_result_size, 
                :sel_sql_cache, :sel_sql_calc_found_rows

    VALID_JOINS = [ "JOIN", "INNER JOIN", "CROSS JOIN", "LEFT JOIN", "RIGHT JOIN", 
                    "LEFT OUTER JOIN", "RIGHT OUTER_JOIN",
                    "NATURAL JOIN JOIN", "NATURAL LEFT JOIN", "NATURAL RIGHT JOIN", 
                    "NATURAL LEFT OUTER JOIN", "NATURAL RIGHT OUTER JOIN" ]
    METHODS = {
                :straight_join => { :attr => 'sel_straight_join', :name => 'STRAIGHT_JOIN' },
                :sql_cache     => { :attr => 'sel_sql_cache',     :name => 'SQL_CACHE'     },
                :sql_no_cache  => { :attr => 'sel_sql_cache',     :name => 'SQL_NO_CACHE'  },
                :high_priority => { :attr => 'sel_high_priority', :name => 'HIGH_PRIORITY' },
                :sql_calc_found_rows => { 
                                            :attr => 'sel_sql_calc_found_rows', 
                                            :name => 'SQL_CALC_FOUND_ROWS'
                                        },
                :sql_small_result => { :attr => 'sel_sql_result_size', :name => 'SQL_SMALL_RESULT'  },
                :sql_big_result   => { :attr => 'sel_sql_result_size', :name => 'SQL_BIG_RESULT'    },
                :sql_buffer_result=> { :attr => 'sel_sql_result_size', :name => 'SQL_BUFFER_RESULT' },
              }

    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    #   *list       - list of sources for the FROM clause
    ##########################################################################
    def initialize ( _caller, *list )
        super
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


class SQLConstructor::BasicDelete_mysql < SQLConstructor::BasicDelete

    attr_reader :del_ignore, :del_quick, :del_low_priority

    METHODS = {
                :low_priority => { :attr => 'del_low_priority', :name => 'LOW_PRIORITY' },
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
