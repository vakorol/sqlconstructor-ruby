
class SQLException < RuntimeError

    INVALID_COL_NAME        = "Invalid column name."
    INVALID_RULES           = "No valid syntax rules found for dialect"
    NUMERIC_VALUE_EXPECTED  = "Numeric argument value expected."
    UNKNOWN_DIALECT         = "Unimplemented SQL dialect specified"
    UNKNOWN_METHOD          = "Unknown method called"
    UNKNOWN_OPERATOR_TYPE   = "Unknown conditional operator type."
    VALUES_NUM_MISMATCH     = "Number of values provided mismatches the number of requested columns."
    WHERE_INVALID_ARGS      = "Hash expected in .where() arguments."
    WHERE_LIKE_INVALID_ARGS = "Hash with scalar values expected in .where() arguments."
 
    def initialize ( msg )
        @msg = msg
    end

    def to_s
        return @msg
    end

end
