# $Id: xmlgen.rb,v 1.1 2006/01/04 02:03:17 francis Exp $
# by Michael Neumann

module DBI; module Utils; module XMLFormatter 
  class << self
    public 
    begin
      require "rexml/document"

      def row_to_xml(row, rowtag="row", include_nulls=true, colmap={})
        entry = REXML::Element.new rowtag
        row.each_with_name do |val, name|
          next if not include_nulls and val.nil?
          add_rec(val.to_s, entry, (colmap[name] || name).split("/")) 
        end
        entry
      end

      def table_to_xml(rows, roottag="rows", rowtag="row", include_nulls=true, colmap={}) 
        root = REXML::Element.new roottag
        rows.each do |row|
          root << row_to_xml(row, rowtag, include_nulls, colmap)
        end
        root
      end

      private # -------------------------------------------------------------------------

      def add_rec(row_value, elem, sub)
        name, rest = sub 
        if sub.nil? or sub.empty?
          elem.add_text row_value
        elsif name =~ /^@/
          elem.add_attribute $', row_value
        else
          e = elem.elements[name]
          if e
            add_rec(row_value, e, rest)
          else
            new_elem=REXML::Element.new(name)
            add_rec(row_value, new_elem, rest)
            elem.add(new_elem)
          end 
        end
      end

    rescue LoadError
    end

  end # class self
end end end
