
##########################################################################################
#   MySQL dialect descendant of BasicSelect class
##########################################################################################
class SQLConstructor::BasicSelect_mysql < SQLConstructor::BasicSelect

    attr_reader :sel_high_priority, :sel_straight_join, :sel_sql_result_size, 
                :sel_sql_cache, :sel_sql_calc_found_rows, :gen_limit, :sel_group_by_with_rollup

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
                :limit            => { :attr => 'gen_limit', :name => 'LIMIT', :val => SQLObject },
                :group_by_with_rollup => { 
                                            :attr => 'sel_group_by_with_rollup', 
                                            :name => "WITH ROLLUP" 
                                         }
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


##########################################################################################
#   MySQL dialect descendant of BasicDelete class
##########################################################################################
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
    end

end 


##########################################################################################
#   MySQL dialect descendant of BasicInsert class
##########################################################################################
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


##########################################################################################
#   MySQL dialect descendant of BasicUpdate class
##########################################################################################
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


##########################################################################################
#   MySQL dialect descendant of BasicJoin class
##########################################################################################
class SQLConstructor::BasicJoin_mysql < SQLConstructor::BasicJoin

    VALID_JOINS = [ "join", "inner_join", "cross_join", "left_join", "right_join", 
                    "left_outer_join", "right_outer_join",
                    "natural_join", "natural_left_join", "natural_right_join", 
                    "natural_left_outer_join", "natural_right_outer_join" ]

    #############################################################################
    #   Class contructor. Takes a caller object as the first argument, JOIN 
    #   type as the second argument, and a list of sources for the JOIN clause
    #############################################################################
    def initialize ( _caller, type, *sources )
        type = type.to_s
        if ! VALID_JOINS.include? type
            raise NoMethodError, ERR_UNKNOWN_METHOD + ": " + type
        end

        super
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
  
