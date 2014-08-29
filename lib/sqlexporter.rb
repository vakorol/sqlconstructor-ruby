
##############################################################################################
#   This class implements the interface for exprting SQLConstructor objects to strings.
##############################################################################################
class SQLExporter

    attr_accessor :dialect, :tidy
    attr_reader :separator

     # defaults to 'mysql'
    DEFAULT_DIALECT = 'mysql'

    #############################################################################
    #   Class constructor. Called with two optional arguments - dialect and tidy.
    #   Dialect determines the translator class (e.g., Exporter_mysql, 
    #   Exporter_informix etc). Tidy determines whether the output should be
    #   formatted and human-readable.
    #############################################################################
    def initialize ( dialect = DEFAULT_DIALECT, tidy = false )
        dialect ||= DEFAULT_DIALECT
        dialect_class = "Exporter_" + dialect.to_s
        begin
            @translator = SQLExporter.const_get( dialect_class ).new( tidy )
        rescue
            raise NameError, ERR_UNKNOWN_DIALECT + ": " + dialect.to_s
        end
        @dialect, @tidy = dialect, tidy
        @separator = @translator.separator
    end

    #############################################################################
    #   The main method to export the obj to string. Calls the @translator's 
    #   export method.
    #############################################################################
    def export ( obj )
        string = @translator.export obj
        string = @separator + "(" + string + ")"  if obj.inline
        return string
    end


  ######################################################################################### 
  #   The exporter class of a 'generic' sql dialect. This should be the parent for
  #   all dialect-specific exporter classes.
  ######################################################################################### 
    class Exporter_generic

        attr_reader :separator

        #############################################################################
        #   Class constructor.
        #############################################################################
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
                raise NameError, ERR_INVALID_RULES + " '" + self.send( :dialect ) + "'"
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

        #############################################################################
        #   Generic representation of the JOIN clause
        #############################################################################
        def gen_joins ( obj )
            arr_joins = [ ]
            if obj.gen_joins 
                arr_joins = obj.gen_joins.map { |join|  join.to_s }
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
            if _attr.is_a? Array
                result += _attr.join @separator
            elsif _attr
                result += _attr.to_s
            end
 
            return result
        end

    end

end

##################################################################################################
##################################################################################################
#   Include dialect-specific classes from ./dialects/exporter/  :
#   This should be done after SQLExporter is defined.
##################################################################################################
##################################################################################################

Dir[ DIALECTS_PATH + "/*-exporter.rb"].each { |file|  require file }
 
