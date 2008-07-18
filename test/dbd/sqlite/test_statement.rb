class TestSQLiteStatement < DBDConfig.testbase(:sqlite)
    def test_constructor
        sth = DBI::DBD::SQLite::Statement.new("select * from foo", @dbh.instance_variable_get("@handle"))

        assert_kind_of DBI::DBD::SQLite::Statement, sth
        assert sth.instance_variable_get("@dbh")
        assert_kind_of DBI::DBD::SQLite::Database, sth.instance_variable_get("@dbh")
        assert_equal(@dbh.instance_variable_get("@handle"), sth.instance_variable_get("@dbh"))
        assert_kind_of DBI::SQL::PreparedStatement, sth.instance_variable_get("@statement")
        assert_equal({ }, sth.instance_variable_get("@attr"))
        assert_equal([ ], sth.instance_variable_get("@params"))
        assert_nil(sth.instance_variable_get("@result_set"))
        assert_equal([ ], sth.instance_variable_get("@rows"))

        sth = @dbh.prepare("select * from foo")

        assert_kind_of DBI::StatementHandle, sth
    end

    def test_bind_param
        sth = DBI::DBD::SQLite::Statement.new("select * from foo", @dbh.instance_variable_get("@handle"))

        assert_raise(DBI::InterfaceError) do
            sth.bind_param(:foo, "monkeys")
        end

        # XXX this is fairly ugly, but...
        # what i've attempted to do here is normalize what is tested, even
        # though the data differs subtly.  you'll notice that there are two
        # arrays that get passed to the each block for evaluation.  the first
        # argument is the statment handle (raw from SQLite DBD or the facade
        # from DBI), the second is how we access the @params internally held
        # variable, and the third is how these params are scrubbed before we
        # assert against them.
        #
        # the @params variable is in different spots in both statement handles
        # and the values of the params are quoted differently. However, the
        # full pipe works and I'd like to ensure that both do their job as a
        # team.
        #
        [ 
          [ 
              sth, 
              proc { |x| x.instance_variable_get("@params") }, 
              proc { |x| x } 
          ],
          [ 
              @dbh.prepare("select * from foo"), 
              proc { |x| x.instance_variable_get("@handle").instance_variable_get("@params") },
              proc { |x| x.gsub(/(^')|('$)/, '') }
          ]
        ].each do |sthpack|
            sthpack[0].bind_param(1, "monkeys", nil)

            params = sthpack[1].call(sthpack[0])
            
            assert_equal "monkeys", sthpack[2].call(params[0])

            # set a bunch of stuff.
            %w(I like monkeys).each_with_index { |x, i| sthpack[0].bind_param(i+1, x) }

            params = sthpack[1].call(sthpack[0])
            
            assert_equal %w(I like monkeys), params.collect { |x| sthpack[2].call(x) }

            # FIXME what to do with attributes? are they important in SQLite?
        end
    end
    
    def test_column_info
        sth = nil
        
        assert_nothing_raised do 
            sth = @dbh.prepare("select * from names")
            sth.execute
        end

        assert_kind_of Array, sth.column_info 
        assert_kind_of DBI::ColumnInfo, sth.column_info[0]
        assert_kind_of DBI::ColumnInfo, sth.column_info[1]
        assert_equal [ 
            { 
                "name" => "name",
                "sql_type" => 12,
                "precision" => 255,
                "type_name" => "varchar"
            }, 
            { 
                "name" => "age",
                "sql_type" => 4,
                "type_name" => "integer"
            } 
        ], sth.column_info

        sth.finish
    end
end
