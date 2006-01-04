#
# $Id: persistence.rb,v 1.1.1.1 2006/01/04 02:03:17 francis Exp $
# Copyright (c) 2001 by Michael Neumann
#

require "dbi"

# TODO:
#  store_all
#  refresh_all


class Persistence

  def initialize(tablename, dbd_class, indices={}, &p)
    @tablename = tablename
    @indices   = indices
    instance_eval(&p) if block_given?
    @dbd = dbd_class.new(@tablename, @indices)
  end

  def add(klass)

    klass.class_eval %{
      attr_accessor :__oid

      def delete
	self.class.delete(self)
      end

      def store
	self.class.store(self)
      end

      def reget
	self.class.reget(self)
      end
    }

    klass.extend Enumerable

    def klass.commit
      @connection.commit
    end

    def klass.rollback
      @connection.rollback
    end

    def klass.connection
      @connection
    end
    def klass.connection=(val)
      @connection = val
    end

    def klass.init(dbd, ind, tabl)
      @connection, @dbd, @indices, @tablename = nil, dbd, ind, tabl
    end

    def klass.get_objects(where=nil, *params)
      obj = []
      sql = "SELECT obj_data FROM #{@tablename}"
      sql << " WHERE #{where}" unless where.nil?
      @connection.select_all(sql, *params) {|row| obj << Marshal.load(row["obj_data"]) }
      return *obj
    end


    def klass.get_where(where, *params)
      get_objects(where, *params)
    end

    def klass.get_all
      get_objects(nil)
    end

    def klass.each(&p)
      get_all.each( &p )
    end


    # DO NOT Update the internal state, BUT returns a new object
    def klass.reget(obj)
      raise "Store object before you reget/refresh it"  if obj.__oid.nil?
      obj = @connection.select_one("SELECT obj_data FROM #{@tablename} WHERE obj_id = #{obj.__oid}")
      Marshal.load(obj["obj_data"]) 
    end

    def klass.delete(obj)
      if obj.__oid.nil?
	raise "Object is not in table!" 
      else
	@connection.do("DELETE FROM #{@tablename} WHERE obj_id = #{obj.__oid}") 
	obj.__oid = nil
      end
      obj
    end

    def klass.store(obj)
      names  = @indices.collect {|name, type| name}
      values = @indices.collect {|name, type| obj.send(name)} 

      if obj.__oid.nil?
	# new object => insert
        obj_id   = obj.__oid = @dbd.get_new_obj_id(@connection)
	obj_data = Marshal.dump(obj)  

	if names.empty?
	  strnames = ""
	else
	  strnames = ", " + names.join(",")
	end

	sql = %{
	  INSERT INTO #{@tablename} (obj_id, obj_data #{strnames})
	  VALUES (#{(["?"] * (2+names.size)).join(",")})
	}

	@connection.do(sql, obj_id, obj_data, *values)
      else
	# update

	obj_id   = obj.__oid
	obj_data = Marshal.dump(obj)  

	if names.empty?
	  strnames = ""
	else
	  strnames = ", " + names.collect{|n| "#{n} = ?"}.join(",")
	end

	@connection.do( %{
	  UPDATE #{@tablename} SET obj_data = ?
	  #{strnames} WHERE obj_id = #{obj_id}
	}, obj_data, *values)
      end
      obj
    end

    def klass.install
      ind = @indices.collect {|field, type| ", #{field} #{type}"}.join("\n")
     
      # remove old tables and sequences
      uninstall

      @dbd.install(@connection, ind)
    end

    def klass.installed?
      begin
	@connection.do("SELECT NULL FROM #{@tablename}")
        true
      rescue DBI::Error
        false
      end
    end

    def klass.uninstall
      @dbd.uninstall_other(connection)
      begin
	@connection.do("DROP TABLE #{@tablename}")
      rescue DBI::Error
      end
    end


    klass.init(@dbd, @indices, @tablename)

  end # method add

  private # ---------------------------------------------------------------------

  def index_on(field, type)
    @indices[field] = type
  end


  # Subclasses -------------------------------------------------------------------

  class Base
    def initialize(tablename, indices)
      @tablename, @indices = tablename, indices
    end
  end

  class Oracle < Base
    def get_new_obj_id(connection)
       connection.select_one("SELECT seq_#{@tablename}.NEXTVAL AS obj_id")["obj_id"] 
    end

    def install(connection, ind)
      connection.do %{
	CREATE SEQUENCE seq_#{@tablename}
      } 

      connection.do %{
	CREATE TABLE #{@tablename} (
	    obj_id INT NOT NULL
	  #{ind} 
	  , obj_data LONG RAW 
	  , PRIMARY KEY (obj_id)
	)
      }
    end
    def uninstall_other(connection)
      connection.do("DROP SEQUENCE seq_#{@tablename}")
    rescue DBI::Error
    end

  end # class Oracle

  class Pg < Base
    def get_new_obj_id(connection)
       connection.select_one("SELECT nextval('seq_#{@tablename}') AS obj_id")["obj_id"] 
    end

    def install(connection, ind)
      connection.do %{
	CREATE SEQUENCE seq_#{@tablename}
      } 

      connection.do %{
	CREATE TABLE #{@tablename} (
	    obj_id INT NOT NULL
	  #{ind} 
	  , obj_data TEXT
	  , PRIMARY KEY (obj_id)
	)
      }
    end
    def uninstall_other(connection)
      connection.do("DROP SEQUENCE seq_#{@tablename}")
    rescue DBI::Error
    end
  end # class Pg



end # class Persistence

