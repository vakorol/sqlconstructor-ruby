
class SQLExporter

    attr_accessor :dialect, :tidy

    DEFAULT_DIALECT = :mysql
    VALID_DIALECTS  = [ 'mysql', 'informix' ]


    def initialize ( dialect = DEFAULT_DIALECT, tidy = false )
        dialect ||= DEFAULT_DIALECT
        if ! VALID_DIALECTS.include? dialect 
            raise NameError, SQLException::UNKNOWN_DIALECT + ": " + dialect.to_s
        end
        @dialect = dialect
        @tidy    = tidy
        dialect_class = "Dialect_" + dialect.to_s
        @translator = SQLExporter.const_get( dialect_class ).new( tidy )
    end


    def print ( obj )
        if obj.is_a? SQLConstructor::GenericQuery
            @translator.export obj
        else
            obj.to_s
        end
    end


  ###########################################################################################
  ############################### INTERNAL CLASSES START HERE ###############################
 
    class Dialect_generic

         # The main rule for the generic DELETE query syntax
        DELETE_SYNTAX = [
                            "DELETE",
                            :gen_from,
                            :gen_where,
                            :gen_order_by,
                            :gen_limit
                        ]
 

        def initialize ( tidy )
            @tidy = tidy
            @separator = @tidy ? "\n" : " "
        end


        def separator
            @tidy ? "\n" : " "
        end


        #############################################################################
        #   Exports a string with a query from object.
        #   This method should be called from a child class with defined constant
        #   arrays [SELECT|DELETE|UPDATE|INSERT]_SYNTAX. Methods defined in the array 
        #   are called in the specified order for the object obj.
        #############################################################################
        def export ( obj )
            rules_const_name = obj.class.name.sub( /^.+?::Basic([^_]+).+/, '\1' ).upcase + "_SYNTAX"
            begin
                rules = self.class.const_get( rules_const_name.to_sym )
            rescue NameError
                raise NameError, SQLException::INVALID_RULES + " '" + self.send( :dialect ) + "'"
            end
            string = ""
            rules.each do |rule|
                if rule.is_a? String
                    string += rule + self.separator
                elsif rule.is_a? Symbol
                    res = self.send( rule, obj ).to_s
                    string += res
                    string += self.separator  if ! res.empty?
                end
            end
            return string
        end

        
        def sel_distinction ( obj )
            return ( obj.attr_distinction || "" )
        end

 
        def gen_expression ( obj )
            return obj.attr_objects.join ","
        end


        def gen_from ( obj )
            result = "FROM " + Helper.printAliasHash( obj.from_hash )  if obj.from_hash
            return ( result || "" )
        end

        
        def gen_joins ( obj )
            result = ""
            if obj.joins 
                obj.joins.each do |join|
                    result += join.type + " " + Helper.printAliasHash( join.sources )
                    result += self.separator
                    result += "ON " + join.on_obj.to_s  if join.on_obj
                end
            end
            return result
        end


        def gen_where ( obj )
            return ( obj.attr_where  ? "WHERE "+ obj.attr_where.to_s  : "" )
        end


        def sel_group_by ( obj )
            string = ""
            if obj.attr_group_by
                string  = "GROUP BY " + obj.attr_group_by[:list].join( "," )
                string += " " + obj.attr_group_by[:order]  if obj.attr_group_by[:order]
            end
        end

        def gen_order_by ( obj )
            string = ""
            if obj.attr_order_by
                string  = "ORDER BY " + obj.attr_order_by[:list].join( "," )
                string += " " + obj.attr_order_by[:order]  if obj.attr_order_by[:order]
            end
        end

        def sel_union ( obj )
            result = ''
            if obj.attr_union
                result = obj.attr_union[:type] + " " + obj.attr_union[:object].to_s
            end
            return result
        end
 
                                                                                    
        #############################################################################
        #   Returns empty string for all undefined methods, so that a corrupt syntax
        #   rule could be just ignored.
        #############################################################################
        def method_missing ( method, *args )
            obj = args[0]
            return ''  if ! obj.is_a? SQLObject
            result = ""

            _attr = obj.send method
            if _attr
                result += _attr[:name]
                result += ' '  if _attr[:type] != :function
                if _attr[:val].is_a? SQLList
                    result += _attr[:val].to_s
                elsif _attr[:val]
                    _attr[:val] = [ _attr[:val] ]  if ! _attr[:val].is_a? Array
                    _attr[:val].each { |val|  result += val.to_s }
                end
            end
 
            return result
        end

    end

 
  ###########################################################################################
  #################### INDIVIDUAL DIALECT TRANSLATOR CLASSES START HERE #####################
  ###########################################################################################  


    #####################################################################################
    #   MySQL dialect translator class.
    #####################################################################################
    class Dialect_mysql < Dialect_generic

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
                            :gen_expression,
                            :gen_from,    
                            :gen_joins,   
                            :gen_where,   
                            :sel_group_by,
                            :sel_having,  
                            :gen_order_by,
                            :gen_limit,
                            :sel_union
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
            if obj.attr_from
                result = "FROM " + Helper.to_sWithAliasesIndexes( obj, :attr_from )
            end
            return result
        end
 

        #############################################################################
        #   Forms a string for all JOINs for an object. Index hints included.
        #############################################################################
        def gen_joins ( obj )
            arr_joins = [ ]
            if obj.attr_joins 
                obj.attr_joins.each do |join|
                    result  = join.type + " " + Helper.to_sWithAliasesIndexes( join, :sources )
                    result += self.separator
                    result += "ON " + join.on_obj.to_s  if join.on_obj
                    arr_joins << result
                end
            end
            return arr_joins.join( self.separator )
        end
 

      ### MySQL-specific methods

        def sel_high_priority ( obj )
            return ( obj.attr_high_priority || "" )
        end
 
        def sel_straight_join ( obj )
            return ( obj.attr_straight_join || "" )
        end

        def sel_result_size ( obj )
            return ( obj.attr_sql_result_size || "" )
        end

        def sel_cache ( obj )
            return ( obj.attr_sql_cache || "" )
        end

        def sel_calc_found_rows ( obj )
            return ( obj.attr_sql_calc_found_rows || "" )
        end

        def sel_having ( obj )
            result = ""
            result += "HAVING " + obj.attr_having.to_s  if obj.attr_having
            return result
        end
 
        def gen_limit ( obj )
            result = ""
            if obj.attr_first
                result += "LIMIT "           
                result += obj.attr_skip.to_s + ","  if obj.attr_skip
                result += obj.attr_first.to_s
            end
            return result
        end
 
    end


    #####################################################################################
    #   IBM Informix dialect translator class.
    #####################################################################################
    class Dialect_informix < Dialect_generic

        attr_reader :dialect


        def initialize ( tidy )
            @tidy = tidy
            @dialect = 'informix'
            super
        end


        def printSelect ( obj )
            string = " SELECT "
            string += " SKIP "  + obj.attr_skip   if obj.attr_skip
            string += " FIRST " + obj.attr_first  if obj.attr_first
            string += super
            string += obj.joins.each{ |join|  printJoin join }       if obj.joins
            string += " WHERE " + obj.attr_where.to_s                 if obj.attr_where
            string += " GROUP BY " + obj.attr_group_by.join( ", " )  if obj.attr_group_by
            string += " ORDER BY " + obj.attr_order_by.join( ", " )  if obj.attr_order_by
            string += super.printUnion( obj )   if obj.attr_union
            return string
        end

    end
    
end
