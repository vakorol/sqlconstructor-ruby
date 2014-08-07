
##############################################################################################
#   This class implements the interface for exprting SQLConstructor objects to strings.
##############################################################################################
class SQLExporter

    attr_accessor :dialect, :tidy
    attr_reader :separator

    DEFAULT_DIALECT = :mysql

    #############################################################################
    #   Class constructor. Called with two optional arguments - dialect and tidy.
    #   Dialect determines the translator class (e.g., Exporter_mysql, 
    #   Exporter_informix etc). Tidy determines whether the output should be
    #   formatted and human-readable.
    #############################################################################
    def initialize ( dialect = DEFAULT_DIALECT, tidy = false )
        dialect ||= DEFAULT_DIALECT
        dialect_class = "Dialect_" + dialect.to_s
        begin
            @translator = SQLExporter.const_get( dialect_class ).new( tidy )
        rescue
            raise NameError, SQLException::UNKNOWN_DIALECT + ": " + dialect.to_s
        end
        @dialect = dialect
        @tidy    = tidy
        @separator = @translator.separator
    end

    #############################################################################
    #   The main method to export the obj to string. Calls the @translator's 
    #   export method.
    #############################################################################
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

        attr_reader :separator

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

        #############################################################################
        #   exports a string with a query from object.
        #   this method should be called from a child class with defined constant
        #   arrays [select|delete|update|insert]_syntax. methods defined in the array 
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
                    string += rule + @separator
                elsif rule.is_a? Symbol
                    res = self.send( rule, obj ).to_s
                    string += res
                    string += @separator  if ! res.empty?
                end
            end
            return string
        end

 
        def gen_joins ( obj )
            arr_joins = [ ]
            if obj.gen_joins 
                obj.gen_joins.each do |join|
                    result += join.type + " " + join.join_sources.to_s
                    result += @separator
                    result += "ON " + join.join_on.to_s  if join.join_on
                    arr_joins << result
                end
            end
            return arr_joins.join( @separator )
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
                if [ SQLValList, SQLAliasedList ].include? _attr[:val]
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

Dir["./dialects/*-exporter.rb"].each { |file| require file }
 
