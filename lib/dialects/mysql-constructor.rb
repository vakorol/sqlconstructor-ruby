
VALID_INDEX_HINTS = [ :use_index, :force_index, :ignore_index, 
                      :use_key, :ignore_key, :force_key ]
 
##########################################################################################
#   MySQL dialect descendant of BasicSelect class
##########################################################################################
class SQLConstructor::BasicSelect_mysql < SQLConstructor::BasicSelect

    attr_reader :attr_high_priority, :attr_straight_join, :attr_sql_result_size, 
                :attr_sql_cache, :attr_sql_calc_found_rows, :attr_limit, :attr_group_by_with_rollup
    
    METHODS = {
      :straight_join => SQLConstructor::QAttr.new( :name => 'attr_straight_join', 
                                                   :text => 'STRAIGHT_JOIN' ),
      :sql_cache => SQLConstructor::QAttr.new( :name => 'attr_sql_cache', :text => 'SQL_CACHE'),
      :sql_no_cache => SQLConstructor::QAttr.new( :name => 'attr_sql_cache', :text => 'SQL_NO_CACHE'),
      :high_priority => SQLConstructor::QAttr.new( :name => 'attr_high_priority',:text => 'HIGH_PRIORITY' ),
      :sql_calc_found_rows  => SQLConstructor::QAttr.new( :name => 'attr_sql_calc_found_rows', 
                                                          :text => 'SQL_CALC_FOUND_ROWS' ),
      :sql_small_result     => SQLConstructor::QAttr.new( :name => 'attr_sql_result_size', 
                                                          :text => 'SQL_SMALL_RESULT' ),
      :sql_big_result       => SQLConstructor::QAttr.new( :name => 'attr_sql_result_size', 
                                                          :text => 'SQL_BIG_RESULT' ),
      :sql_buffer_result    => SQLConstructor::QAttr.new( :name => 'attr_sql_result_size', 
                                                          :text => 'SQL_BUFFER_RESULT' ),
      :limit                => SQLConstructor::QAttr.new( :name => 'attr_limit', 
                                                          :text => 'LIMIT', :val => SQLObject ),
      :group_by_with_rollup => SQLConstructor::QAttr.new( :name => 'attr_group_by_with_rollup', 
                                                          :text => "WITH ROLLUP" ),
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
    #   Send missing methods calls to the @caller object, and also handle
    #   JOINs, UNIONs and INDEX hints
    ##########################################################################
    def method_missing ( method, *args )
         # Handle all [*_]join calls:
        return _addJoin( method, *args )  if method.to_s =~ /^[a-z_]*join$/
          # Handle all valid *_index/*_key calls:
        return _addIndexes( method, *args )  if VALID_INDEX_HINTS.include? method 
        super
    end

  #########
  private
  #########

    ##########################################################################
    #   Adds a USE/FORCE/IGNORE INDEX clause for the last objects in for_vals
    #   argument.
    ##########################################################################
    def _addIndexes ( type, *list )
        type = type.to_s
        type.upcase!.gsub! /_/, ' '
        @attr_index_hints ||= [ ]
         # set the attr_index_hints for the last object in for_vals
        last_ind = @attr_from.val.length - 1
        @attr_index_hints[last_ind] = { :type => type, :list => SQLObject.get( list ) }
        @string = nil
        return self
    end
    
end


##########################################################################################
#   MySQL dialect descendant of BasicDelete class
##########################################################################################
class SQLConstructor::BasicDelete_mysql < SQLConstructor::BasicDelete

    attr_reader :del_ignore, :del_quick, :del_low_priority, :attr_limit

    METHODS = {
     :low_priority => SQLConstructor::QAttr.new( :name => 'del_low_priority', 
                                                 :text => 'LOW_PRIORITY' ),
     :quick        => SQLConstructor::QAttr.new( :name => 'del_quick',  :text => 'QUICK '),
     :ignore       => SQLConstructor::QAttr.new( :name => 'del_ignore', :text => 'IGNORE'),
     :limit => SQLConstructor::QAttr.new( :name => 'attr_limit', :text => 'LIMIT', :val => SQLObject)
    }
 
    ##########################################################################
    #   Class constructor. 
    #   _caller     - the caller object
    ##########################################################################
    def initialize ( _caller )
        super
    end

    ##########################################################################
    #   Handle JOINs or send call to the parent.
    ##########################################################################
    def method_missing ( method, *args )
         # Handle all [*_]join calls:
        return _addJoin( method, *args )  if method =~ /^[a-z_]*join$/
        super
    end
 
end 


##########################################################################################
#   MySQL dialect descendant of BasicInsert class
##########################################################################################
class SQLConstructor::BasicInsert_mysql < SQLConstructor::BasicInsert

    attr_reader :ins_priority, :ins_quick, :ins_ignore, :ins_on_duplicate_key_update

    METHODS = {
     :low_priority  => SQLConstructor::QAttr.new( :name => 'ins_priority', :text => 'LOW_PRIORITY' ),
     :delayed       => SQLConstructor::QAttr.new( :name => 'ins_priority', :text => 'DELAYED'       ),
     :high_priority => SQLConstructor::QAttr.new( :name => 'ins_priority', :text => 'HIGH_PRIORITY' ),
     :quick         => SQLConstructor::QAttr.new( :name => 'ins_quick',    :text => 'QUICK'         ),
     :ignore        => SQLConstructor::QAttr.new( :name => 'ins_ignore',   :text => 'IGNORE'        ),
     :on_duplicate_key_update => SQLConstructor::QAttr.new( 
                                   :name => 'ins_on_duplicate_key_update', 
                                   :text => 'ON DUPLICATE KEY UPDATE',
                                   :val  => SQLConditional
                                 )
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

    attr_reader :upd_low_priority, :upd_ignore, :attr_limit

    METHODS = {
     :low_priority => SQLConstructor::QAttr.new( :name => 'upd_low_priority', :text => 'LOW_PRIORITY'),
     :ignore       => SQLConstructor::QAttr.new( :name => 'upd_ignore', :text => 'IGNORE' ),
     :limit        => SQLConstructor::QAttr.new( :name => 'attr_limit', :text => 'LIMIT', 
                                                 :val => SQLObject )
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

    ##########################################################################
    #   Class contructor. Takes a caller object as the first argument, JOIN 
    #   type as the second argument, and a list of sources for the JOIN clause
    ##########################################################################
    def initialize ( _caller, type, *sources )
        type = type.to_s
        if ! VALID_JOINS.include? type
            raise NoMethodError, ERR_UNKNOWN_METHOD + ": " + type
        end
        super
    end

    ##########################################################################
    #   Adds a USE/FORCE/IGNORE INDEX clause for the last objects in for_vals
    #   argument.
    ##########################################################################
    def _addIndexes ( type, *list )
        type = type.to_s
        type.upcase!.gsub! /_/, ' '
        @attr_index_hints ||= [ ]
         # set the attr_index_hints for the last object in for_vals
        last_ind = @join_sources.length - 1
        @attr_index_hints[last_ind] = { :type => type, :list => SQLObject.get( list ) }
        @string = nil
        return self
    end

    ##########################################################################
    #   Export to string with index hints included
    ##########################################################################
    def to_s
        return @string  if @string
        result  = @type + " " + to_sWithAliasesIndexes( @join_sources )
        result += @exporter.separator
        result += "ON " + @join_on.val.to_s  if @join_on
        @string = result
    end
 
    ##########################################################################
    #   Handles INDEX hints or sends the call to the parent
    ##########################################################################
    def method_missing ( method, *args )
         # Handle all valid *_index/*_key calls:
        return _addIndexes( method, *args )  if VALID_INDEX_HINTS.include? method 
        super
    end

  ########
  private
  ########

    ##########################################################################
    #   Returns a string of objects in list merged with @attr_index_hints
    ##########################################################################
    def to_sWithAliasesIndexes ( list )
        list = [ list ]  if ! [ Array, SQLValList, SQLAliasedList ].include? list.class
        arr  = [ ]
        list.each_with_index do |item,i|
            _alias = item.alias ? " " + item.alias.to_s : ""
            str = item.to_s + _alias
            if @attr_index_hints
                index_hash = @attr_index_hints[i]
                str += " " + index_hash[:type] + " " + index_hash[:list].to_s
            end
            arr << str
        end
        return arr.join ','
    end
 
end
  
