
    #####################################################################################
    #   MySQL dialect translator class.
    #####################################################################################
    class SQLExporter::Exporter_mysql < SQLExporter::Exporter_generic

        attr_reader :dialect

         # The main rule for the MySQL SELECT query syntax
        SELECT_SYNTAX = [
                            "SELECT",
                            :attr_distinction,
                            :attr_high_priority,
                            :attr_straight_join,
                            :attr_sql_result_size,
                            :attr_sql_cache,
                            :attr_sql_calc_found_rows,
                            :attr_expression,
                            :attr_from,    
                            :attr_joins,   
                            :attr_where,   
                            :attr_group_by,
                            :attr_group_by_order,
                            :attr_group_by_with_rollup,
                            :attr_having,  
                            :attr_order_by,
                            :attr_order_by_order,
                            :attr_limit,
                            :attr_unions
                        ]

         # The main rule for the MySQL DELETE query syntax
        DELETE_SYNTAX = [
                            "DELETE",
                            :del_low_priority,
                            :del_quick,
                            :del_ignore,
                            :attr_from,
                            :attr_where,
                            :attr_order_by,
                            :attr_limit
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
                            :attr_where,
                            :attr_order_by,
                            :attr_limit
                        ]
 

        def initialize ( tidy )
            @tidy = tidy
            @dialect = 'mysql'
            super tidy   # ha ha, "super tidy" :)
        end
                    
        #############################################################################
        #   Forms a string for the FROM clause from the objects attributes @attr_from
        #   and @attr_index_hints
        #############################################################################
        def attr_from ( obj )
            result = ""
            if obj.attr_from
                result = "FROM " + to_sWithAliasesIndexes( obj, obj.attr_from.val )
            end
            return result
        end

      ########
      private
      ########

        #############################################################################
        #   Returns a string of objects in list merged with indexes of obj
        #############################################################################
        def to_sWithAliasesIndexes ( obj, list )
            list = [ list ]  if ! [ Array, SQLValList, SQLAliasedList ].include? list.class
            arr  = [ ]
            list.each_with_index do |item,i|
                _alias = item.alias ? " " + item.alias.to_s : ""
                str = item.to_s + _alias
                if obj.attr_index_hints
                    index_hash = obj.attr_index_hints[i]
                    str += " " + index_hash[:type] + " " + index_hash[:list].to_s
                end
                arr << str
            end
            return arr.join ','
        end
 
    end
 
