
class Helper

    def self.getSources( *sources )
        list = [ ]
         # If the list is a hash of objects with aliases:
        if sources.length == 1 && sources[0].is_a?( Hash )
            sources.each do |src, _alias|
                obj = SQLObject.get src
                obj.alias = _alias
                list << obj
            end
         # If the list of FROM sources is an array of objects:
        else
            list = sources.map { |src|  SQLObject.get src }
        end
        return list
    end 


    def self.to_sWithAliases( list )
        arr = list.map{ |obj|  obj.to_s + ( obj.alias  ? " " + obj.alias.to_s  : "" ) }
        return arr.join ','
    end


    def self.to_sWithAliasesIndexes( obj, list )
        list = [ list ]  if ! list.is_a? Array
        arr  = [ ]
        list.each_with_index do |item,i|
            _alias = item.alias ? " " + item.alias.to_d : ""
            str = item.to_s + _alias
            if obj.gen_index_hints
                index_hash = obj.gen_index_hints[i]
                str += " " + index_hash[:type] + " " + index_hash[:list].to_s
            end
            arr << str
        end
        return arr.join ','
    end

end

