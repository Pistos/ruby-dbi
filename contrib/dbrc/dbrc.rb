=begin
= Description
   This is a supplement to the dbi module, allowing you to avoid hard-coding
   passwords in your programs that make database connections.

= Synopsis
   require 'dbi/dbrc'

   dbrc = DBRC.new("mydb")

   or

   dbrc = DBRC.new("mydb","someUser")

   puts dbrc.db
   puts dbrc.user
   puts dbrc.driver
   puts dbrc.timeout
   puts dbrc.max_reconn
   puts dbrc.interval
   puts dbrc.dsn

   dbh = DBI.connect(dbrc.dsn, dbrc.user, dbrc.password)

= Requirements
   The 'etc' module

   Designed for *nix systems.  Untested on Windows.

= Notes on the .dbrc file

   This module relies on a file in your home directory called ".dbrc", and it
   is meant to be analogous to the ".netrc" file used by programs such as telnet.
   The .dbrc file has several conditions that must be met by the module or it
   will fail:

   1) Permissions must be set to 600.
   2) Must be owned by the current user
   3) Must be in the following space-separated format:

      <database> <user> <password> <driver> <timeout> <maximum reconnects> <interval>

   e.g. mydb     dan    mypass     oracle   10       2                    30

   You may include comments in the .dbrc file by starting the line with a "#" symbol

= Class Methods

--- new(db,?user?)
    The constructor takes one or two arguments.  The first argument is the database
    name.  This *must* be provided.  If only the database name is passed, the module
    will look for the first database entry in the .dbrc file that matches.

    The second argument, a user name, is optional.  If it is passed, the module will
    look for the first entry in the .dbrc file where both the database *and* user
    name match.

= Instance Methods

--- <database>
    The name of the database.  Note that the same entry can appear more than once,
    presumably because you have multiple user id's for the same database.

--- <user>
    A valid user name for that database.

--- <password>
    The password for that user.

--- <driver>
    The driver type for that database (Oracle, MySql, etc).

--- <timeout>
    The timeout period for a connection before the attempt is dropped.

--- <maximum reconnects>
    The maximum number of reconnect attempts that should be made for the the
    database.  Presumablly, you would use this with a "retry" within a rescue
    block.

--- <interval>
    The number of seconds to wait before attempting to reconnect to the database
    again should a network/database glitch occur.

--- <dsn>
    Returns a string in "dbi:<driver>:<database>" format.

= Summary

   These "methods" don't really do anything.  They're simply meant as a convenience
   mechanism for you dbi connections, plus a little bit of obfuscation (for passwords).

= Changelog

--- 0.1.1 - 14:54 on 26-Jul-2002
   - Added 'dsn()' method
   - Minor documentation additions

--- 0.1.0 - 12:24 on 26-Jul-2002
   - Initial release

= Author

   Daniel J. Berger
   djberg96@nospam.hotmail.com (remove the 'nospam')

=end

require 'etc'

class DBRC
   attr_accessor :db, :user, :password, :driver, :dsn
   attr_accessor :max_reconn, :timeout, :interval

   def initialize(db,user=nil)
      @dbrc = Etc.getpwuid(Process.uid).dir + "/.dbrc"
      @db = db
      @user = user
      check_file()
      get_info()

      if @user.nil?
         raise "No user entry found for: " + @db
      end

      if @password.nil?
         raise "No password entry found for: " + @db
      end
   end

   #+++++++++++++++++++++++++++++++++
   # Check ownership and permissions
   #+++++++++++++++++++++++++++++++++
   def check_file
      File.open(@dbrc){ |f|

         # Permissions MUST be set to 600
         unless (f.stat.mode & 077) == 0
            raise RuntimeError, "Bad Permissions", caller
         end

         # Only the owner may use it
         unless f.stat.owned?
            raise RuntimeError, "Not Owner", caller
         end
      }
   end

   #+++++++++++++++++++++++++++++++++++++++++++++++++++
   # Grab info out of the .dbrc file.  Ignore comments
   #+++++++++++++++++++++++++++++++++++++++++++++++++++
   def get_info
      f = File.open(@dbrc,"r")
      f.each_line do |line|
         next if line =~ /^#/
         a = line.split('\s+')

         next unless a[0] == @db

         unless @user.nil?
            next unless a[1] == @user
         else
            @user = a[1]
         end

         @password   = a[2]
         @driver     = a[3]
         @timeout    = a[4]
         @max_reconn = a[5]
         @interval   = a[6]

         @dsn = "dbi:" + a[3] + ":" + @db

      end
      f.close
   end
   
end
