
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


    def export ( obj )
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

 
        def gen_joins ( obj )
            arr_joins = [ ]
            if obj.gen_joins 
                obj.gen_joins.each do |join|
                    result += join.type + " " + Helper.printAliasHash( join.join_sources )
                    result += self.separator
                    result += "ON " + join.join_on.to_s  if join.join_on
                    arr_joins << result
                end
            end
            return arr_joins.join( self.separator )
        end

        #############################################################################
        #   Construct an expression string for an object's attribute defined in
        #   in the METHODS constant array.
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
                    result += _attr[:val].join ","
                end
            end
 
            return result
        end

    end

 
  ###########################################################################################
  #################### INDIVIDUAL DIALECT TRANSLATOR CLASSES START HERE #####################
  ###########################################################################################  




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

##################################################################################################
##################################################################################################
#   Include dialect-specific classes from ./dialects/exporter/  :
#   This should be done after SQLExporter is defined.
##################################################################################################
##################################################################################################

Dir["./dialects/exporter/*.rb"].each { |file| require file }
 
