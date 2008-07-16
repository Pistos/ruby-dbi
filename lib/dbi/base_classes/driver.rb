module DBI
   class BaseDriver < Base
       def initialize(dbd_version)
           major, minor = dbd_version.split(".")
           unless major.to_i == DBD::API_VERSION.split(".")[0].to_i
               raise InterfaceError, "Wrong DBD API version used"
           end
       end

       def connect(dbname, user, auth, attr)
           raise NotImplementedError
       end

       def default_user
           ['', '']
       end

       def default_attributes
           {}
       end

       def data_sources
           []
       end

       def disconnect_all
           raise NotImplementedError
       end

   end # class BaseDriver
end
