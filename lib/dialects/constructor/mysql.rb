
class SQLConstructor::BasicSelect_mysql < SQLConstructor::BasicSelect

    attr_reader :sel_high_priority, :sel_straight_join, :sel_sql_result_size, 
                :sel_sql_cache, :sel_sql_calc_found_rows, :gen_limit

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
                :limit            => { :attr => 'gen_limit', :name => 'LIMIT', :val => SQLObject }
              }

    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    #   *list       - list of sources for the FROM clause
    ##########################################################################
    def initialize ( _caller, *list )
        super
    end

end


class SQLConstructor::BasicDelete_mysql < SQLConstructor::BasicDelete

    attr_reader :del_ignore, :del_quick, :del_low_priority, :gen_limit

    METHODS = {
                :low_priority => { :attr => 'del_low_priority', :name => 'LOW_PRIORITY' },
                :quick        => { :attr => 'del_quick',    :name => 'QUICK '       },
                :ignore       => { :attr => 'del_ignore',   :name => 'IGNORE'       },
                :limit        => { :attr => 'gen_limit', :name => 'LIMIT', :val => SQLObject }
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


class SQLConstructor::BasicUpdate_mysql < SQLConstructor::BasicUpdate

    attr_reader :upd_low_priority, :upd_ignore, :gen_limit

    METHODS = {
               :low_priority => { :attr => 'upd_low_priority', :name => 'LOW_PRIORITY' },
               :ignore       => { :attr => 'upd_ignore',       :name => 'IGNORE',      },
               :limit        => { :attr => 'gen_limit', :name => 'LIMIT', :val => SQLObject }
              }

    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    ##########################################################################
    def initialize ( _caller, *list )
        super
    end

end 

 
