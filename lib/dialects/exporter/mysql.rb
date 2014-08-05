
    #####################################################################################
    #   MySQL dialect translator class.
    #####################################################################################
    class SQLExporter::Dialect_mysql < SQLExporter::Dialect_generic

        attr_reader :dialect

         # The main rule for the MySQL SELECT query syntax
        SELECT_SYNTAX = [
                            "SELECT",
                            :sel_distinction,
                            :sel_high_priority,
                            :sel_straight_join,
                            :sel_sql_result_size,
                            :sel_sql_cache,
                            :sel_sql_calc_found_rows,
                            :sel_expression,
                            :gen_from,    
                            :gen_joins,   
                            :gen_where,   
                            :sel_group_by,
                            :sel_group_by_order,
                            :sel_group_by_with_rollup,
                            :sel_having,  
                            :gen_order_by,
                            :gen_order_by_order,
                            :gen_limit,
                            :sel_unions
                        ]

         # The main rule for the MySQL DELETE query syntax
        DELETE_SYNTAX = [
                            "DELETE",
                            :del_low_priority,
                            :del_quick,
                            :del_ignore,
                            :gen_from,
                            :gen_where,
                            :gen_order_by,
                            :gen_limit
                        ]
 
        INSERT_SYNTAX = [
                            "INSERT",
                            :ins_priority,
                            :ins_ignore,
                            :ins_into,
                            :ins_values,
                            :ins_set,
                            :ins_select,
                            :ins_on_duplicate_key_update
                        ]

        UPDATE_SYNTAX = [
                            "UPDATE",
                            :upd_low_priority,
                            :upd_ignore,
                            :upd_tables,
                            :upd_set,
                            :gen_where,
                            :gen_order_by,
                            :gen_limit
                        ]
 

        def initialize ( tidy )
            @tidy = tidy
            @dialect = 'mysql'
            super tidy   # ha ha, "super tidy" :)
        end
                    

      ### Overrides of Dialect_generic methods

        #############################################################################
        #   Forms a string for the FROM clause from the objects attributes @attr_from
        #   and @attr_index_hints
        #############################################################################
        def gen_from ( obj )
            result = ""
            if obj.gen_from
                result = "FROM " + Helper.to_sWithAliasesIndexes( obj, obj.gen_from[:val] )
            end
            return result
        end

        #############################################################################
        #   Forms a string for all JOINs for an object. Index hints included.
        #############################################################################
        def gen_joins ( obj )
            arr_joins = [ ]
            if obj.gen_joins 
                obj.gen_joins.each do |join|
                    result  = join.type + " " + 
                              Helper.to_sWithAliasesIndexes( join, join.join_sources )
                    result += self.separator
                    result += "ON " + join.join_on[:val].to_s  if join.join_on
                    arr_joins << result
                end
            end
            return arr_joins.join( self.separator )
        end
 
    end
 