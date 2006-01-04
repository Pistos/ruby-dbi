#
# setup.rb
#
#   Copyright (c) 2000,2001 Minero Aoki <aamine@dp.u-netsurf.ne.jp>
#
#   This program is free software.
#   You can distribute/modify this program under the terms of
#   the GNU General Public License version 2 or later.
#

require 'tempfile'
if i = ARGV.index(/\A--rbconfig=/) then
  file = $'
  ARGV.delete_at(i)
  require file
else
  require 'rbconfig'
end


class InstallError < StandardError; end


class Installer

  Version   = '2.0.4'
  Copyright = 'Copyright (c) 2000,2001 Minero Aoki'


  TASKS = {
    'config'       => 'save your config configurations',
    'setup'        => 'compiles extention or else',
    'install'      => 'installs packages',
    'clean'        => "does `make clean' for each extention",
    'dryrun'       => 'does test run',
    'show'         => 'shows current configuration'
  }

  TASK_ORDER = %w( config setup install clean dryrun show )

  FILETYPES = %w( bin lib ext share )


  def initialize( argv )
    argv = argv.dup

    @verbose = true
    @no_harm = false

    @config = {}
    @task = nil
    @other_args = []

    @task = parsearg( argv )
    parsearg_TASK @task, argv

    unless @task == 'config' then
      load_configs
      check_packdesig
    end
  end

  attr :config
  attr :task


  ###
  ### arg proc.
  ###

  def parsearg( argv )
    task_re = /\A(?:#{TASKS.keys.join '|'})\z/
    task = nil
    arg = argv.shift

    case arg
    when /\A\w+\z/
      unless task_re === arg then
        raise InstallError, "wrong task: #{arg}"
      end
      task = arg

    when '-h', '--help'
      print_usage $stdout
      exit 0

    when '-v', '--version'
      puts "setup.rb version #{Version}"
      exit 0
    
    when '--copyright'
      puts Copyright
      exit 0

    else
      raise InstallError, "unknown global option '#{arg}'"
    end

    task
  end


  def parsearg_TASK( task, argv )
    mid = "parsearg_#{task}"
    if respond_to? mid, true then
      __send__ mid, argv
    else
      unless argv.empty? then
        raise InstallError, "#{task}:  unknown options: #{argv.join ' '}"
      end
    end
  end

  def parsearg_config( args )
    @config_args = {}
    re = /\A--(#{CONFIG_ORDER.join '|'})=/
    args.each do |i|
      m = re.match(i) or raise InstallError, "config: unknown option #{i}"
      @config_args[ m[1] ] = m.post_match.strip
    end
  end

  def parsearg_install( args )
    args.each do |i|
      if i == '--no-harm' then
        @no_harm = true
      else
        raise InstallError, "#{@task}: wrong option #{i}"
      end
    end
  end

  def parsearg_dryrun( args )
    @dryrun_args = args
  end


  def print_usage( out )
    out.puts
    out.puts 'Usage:'
    out.puts '  ruby setup.rb <global option>'
    out.puts '  ruby setup.rb <task> [<task options>]'

    out.puts
    out.puts 'Tasks:'
    TASK_ORDER.each do |name|
      out.printf "  %-10s  %s\n", name, TASKS[name]
    end

    fmt = "  %-20s %s\n"
    out.puts
    out.puts 'Global options:'
    out.printf fmt, '-h,--help',    'print this message'
    out.printf fmt, '-v,--version', 'print version'
    out.printf fmt, '--copyright',  'print copyright'

    out.puts
    out.puts 'Options for config:'
    CONFIG_ORDER.each do |name|
      dflt, arg, desc, dflt2 = CONFIG_OPTS[name]
      dflt = dflt2 || dflt
      out.printf "  %-20s %s [%s]\n", "--#{name}=#{arg}", desc, dflt
    end
    out.printf "  %-20s %s [%s]\n",
        '--rbconfig=path', 'your rbconfig.rb to load', "running ruby's"

    out.puts
    out.puts 'Options for install:'
    out.printf "  %-20s %s [%s]\n",
        '--no-harm', 'only display what to do if given', 'off'

    out.puts
    out.puts 'This archive includes:'
    out.print '  ', packages().join(' '), "\n"

    out.puts
  end


  ###
  ### tasks
  ###

  def execute
    case @task
    when 'config', 'setup', 'install', 'clean'
      tryto @task
    when 'show'
      do_show
    when 'dryrun'
      do_dryrun
    else
      raise 'must not happen'
    end
  end

  def tryto( task )
    $stderr.printf "entering %s phase...\n", task
    begin
      __send__ 'do_' + task
    rescue
      $stderr.printf "%s failed\n", task
      raise
    end
    $stderr.printf "%s done.\n", task
  end


  ConfigFile = 'config.save'

  def do_config
    CONFIG_OPTS.each do |k,v|
      dflt, vname, desc = v
      @config[k] = dflt
    end

    @config_args.each do |k,v|
      setconf k, v
    end

    save_configs
  end

  def do_show
    CONFIG_ORDER.each do |k|
      v = @config[k]
      if not v or v.empty? then
        v = '(not specified)'
      end
      printf "%-10s %s\n", k, v
    end
  end

  def do_setup
    into_dir( 'bin' ) {
      foreach_package do
        Dir.foreach( '.' ) do |fname|
          next unless File.file? fname
          add_rubypath fname
        end
      end
    }
    into_dir( 'ext' ) {
      foreach_package do
        clean
        extconf
        make
      end
    }
  end

  def do_install
    into_dir( 'bin' ) {
      foreach_package do |targ, *dummy|
        install_bin
      end
    }
    into_dir( 'lib' ) {
      foreach_package do |targ, topfile|
        install_rb targ
        if topfile then
          create_topfile targ, topfile
        end
      end
    }
    into_dir( 'ext' ) {
      foreach_package do |targ, *dummy|
        install_so targ
      end
    }
    into_dir( 'share' ) {
      foreach_package do |targ, *dummy|
        install_dat targ
      end
    }
  end

  def do_clean
    into_dir( 'ext' ) {
      foreach_package do
        clean
      end
    }
    # rmf ConfigFile
  end
  
  def do_dryrun
    unless dir? 'tmp' then
      $stderr.puts 'setup.rb: setting up temporaly environment...'
      @verbose = $DEBUG
      begin
        @config['bin-dir']  = isdir(File.expand_path('.'), 'tmp', 'bin')
        @config['rb-dir']   = isdir(File.expand_path('.'), 'tmp', 'lib')
        @config['so-dir']   = isdir(File.expand_path('.'), 'tmp', 'ext')
        @config['data-dir'] = isdir(File.expand_path('.'), 'tmp', 'share')
        do_install
      rescue
        rmrf 'tmp'
        $stderr.puts '[BUG] setup.rb bug: "dryrun" command failed'
        raise
      end
    end

    exec @config['ruby-path'],
         '-I' + File.join('.', 'tmp', 'lib'),
         '-I' + File.join('.', 'tmp', 'ext'),
         *@dryrun_args
  end
  

  ###
  ### lib
  ###

  #
  # config
  #

  c = ::Config::CONFIG

  rubyname = c['ruby_install_name']
  major = c['MAJOR'].to_i
  minor = c['MINOR'].to_i
  teeny = c['TEENY'].to_i
  version = "#{major}.#{minor}"

  arch = c['arch']

  bindir  = File.join( c['bindir'] )
  rubylib = File.join( c['libdir'], 'ruby' )
  datadir = File.join( c['datadir'] )

  rubypath = File.join( bindir, rubyname )

  # >=1.4.4 is new path
  newpath_p = ((major >= 2) or
               ((major == 1) and
                ((minor >= 5) or
                 ((minor == 4) and (teeny >= 4)))))
  
  if c['rubylibdir'] then
    # 1.6.3 < V
    stdlibdir = c['rubylibdir']
    sitelibdir = c['sitelibdir']
  elsif newpath_p then
    stdlibdir = File.join( rubylib, version )
    sitelibdir = File.join( rubylib, 'site_ruby', version )
  else
    stdlibdir = File.join( rubylib, version )
    sitelibdir = File.join( rubylib, version, 'site_ruby' )
  end

  siterb = sitelibdir
  siteso = File.join( sitelibdir, arch )

  CONFIG_OPTS = {
    'bin-dir'   => [ bindir,
                     'path',
                     'directory to install commands' ],
    'rb-dir'    => [ siterb,
                     'path',
                     'directory to install ruby scripts' ],
    'so-dir'    => [ siteso,
                     'path',
                     'directory to install ruby extentions' ],
    'data-dir'  => [ datadir,
                     'path',
                     'directory to install data' ],
    'ruby-path' => [ rubypath,
                     'path',
                     'path to ruby for #!' ],
    'ruby-prog' => [ rubypath,
                     'path',
                     'path to ruby for installation' ],
    'make-prog' => [ 'make',
                     'name',
                     'make program to compile ruby extentions' ],
    'with'      => [ '',
                     'name,name...',
                     'package name(s) you want to install',
                     'ALL' ],
    'without'   => [ '',
                     'name,name...',
                     'package name(s) you do not want to install' ]
  }

  CONFIG_ORDER = %w( bin-dir rb-dir so-dir ruby-path make-prog with without )

  def save_configs
    File.open( ConfigFile, 'w' ) do |f|
      @config.each do |k,v|
        f.printf "%s=%s\n", k, v if v
      end
    end
  end

  def load_configs
    File.file? ConfigFile or raise InstallError, 'setup.rb config first'
    File.foreach( ConfigFile ) do |line|
      k, v = line.split( '=', 2 )
      setconf k.strip, v.strip
    end
  end

  def setconf( k, v )
    if CONFIG_OPTS[k][1] == 'path' then
      @config[k] = File.expand_path(v)
    else
      @config[k] = v
    end
  end


  #
  # packages
  #

  def check_packdesig
    @with    = extract_dirs( @config['with'] )
    @without = extract_dirs( @config['without'] )

    packs = packages
    (@with + @without).each do |i|
      if not packs.include? i and not dir? i then
        raise InstallError, "no such package or directory '#{i}'"
      end
    end
  end

  def extract_dirs( s )
    ret = []
    s.split(',').each do |i|
      if /[\*\?]/ === i then
        tmp = Dir.glob(i)
        tmp.delete_if {|d| not dir? d }
        if tmp.empty? then
          tmp.push i   # causes error
        else
          ret.concat tmp
        end
      else
        ret.push i
      end
    end

    ret
  end

  def packages
    ret = []
    FILETYPES.each do |type|
      next unless File.exist? type
      foreach_record( "#{type}/PATHCONV" ) do |dir, pack, *dummy|
        ret.push pack
      end
    end
    ret.uniq
  end

  def foreach_package
    path = {}
    foreach_record( './PATHCONV' ) do |dir, pack, targ, topfile, *dummy|
      path[dir] = [pack, targ, topfile]
    end

    base = File.basename( Dir.getwd )
    Dir.foreach('.') do |dir|
      next if dir[0] == ?.
      next unless dir? dir
      next if dir == "CVS"

      path[dir] or raise "abs path for package '#{dir}' not exist"
      pack, targ, topfile = path[dir]

      if inclpack pack, "#{base}/#{dir}" then
        chdir( dir ) {
          yield targ, topfile
        }
      else
        $stderr.puts "setup.rb: skip #{base}/#{dir}(#{pack}) by user option"
      end
    end
  end

  def foreach_record( fname )
    File.foreach( fname ) do |line|
      line.strip!
      next if line.empty?
      a = line.split(/\s+/)
      a[2] ||= '.'
      yield a
    end
  end

  def inclpack( pack, dname )
    if @with.empty? then
      not @without.include? pack and
      not @without.include? dname
    else
      @with.include? pack or
      @with.include? dname
    end
  end


  #
  # setup
  #

  def add_rubypath( fn, opt = nil )
    line = "\#!#{@config['ruby-path']}#{opt ? ' ' + opt : ''}"

    $stderr.puts %Q<setting #! line to "#{line}"> if @verbose
    return if @no_harm

    tmpf = nil
    File.open( fn ) do |f|
      first = f.gets
      return unless /\A\#!.*ruby/ === first

      tmpf = Tempfile.open( 'amsetup' )
      tmpf.puts line
      tmpf << first
      f.each {|i| tmpf << i }
      tmpf.close
    end
    
    mod = File.stat( fn ).mode
    tmpf.open
    File.open( fn, 'w' ) do |wf|
      tmpf.each {|i| wf << i }
    end
    File.chmod mod, fn

    tmpf.close true
  end


  #
  # install
  #

  def install_bin
    install_all isdir(@config['bin-dir']), 0555
  end

  def install_rb( dir )
    install_all isdir(@config['rb-dir'] + '/' + dir), 0644
  end

  def install_dat( dir )
    install_all isdir(@config['data-dir'] + '/' + dir), 0644
  end

  def install_all( dir, mode )
    Dir.foreach('.') do |fname|
      next if /\A\./ === fname
      next unless File.file? fname

      install fname, dir, mode
    end
  end

  def create_topfile( name, req )
    d = isdir(@config['rb-dir'])
    File.open( "#{d}/#{name}.rb", 'w' ) do |f|
      f.puts "require '#{name}/#{req}'"
    end
    File.chmod 0644, "#{d}/#{name}.rb"
  end


  def extconf
    command "#{@config['ruby-prog']} extconf.rb"
  end

  def make
    command @config['make-prog']
  end
  
  def clean
    command @config['make-prog'] + ' clean' if File.file? 'Makefile'
  end

  def install_so( dir )
    to = isdir(File.expand_path(@config['so-dir'] + '/' + dir))
    find_so('.').each do |fn|
      install fn, to, 0555
    end
  end

  DLEXT = ::Config::CONFIG['DLEXT']

  def find_so( dir = '.' )
    fnames = nil
    Dir.open( dir ) {|d| fnames = d.to_a }
    exp = /\.#{DLEXT}\z/
    arr = fnames.find_all {|fn| exp === fn }
    arr or raise InstallError,
            'no ruby extention exists: have you done "ruby setup.rb setup" ?'
  end

  def so_dir?( dn = '.' )
    File.file? "#{dn}/MANIFEST"
  end


  #
  # file op.
  #

  def into_dir( libn )
    return unless dir? libn
    chdir( libn ) {
      yield
    }
  end

  def chdir( dn )
    curr = Dir.pwd
    begin
      Dir.chdir dn
      yield
    ensure
      Dir.chdir curr
    end
  end

  def isdir( dn )
    mkpath dn
    dn
  end

  def mkpath( dname )
    $stderr.puts "mkdir -p #{dname}" if @verbose
    return if @no_harm

    # does not check '/'... it's too abnormal case
    dirs = dname.split(%r_(?=/)_)
    if /\A[a-z]:\z/i === dirs[0] then
      disk = dirs.shift
      dirs[0] = disk + dirs[0]
    end
    dirs.each_index do |idx|
      path = dirs[0..idx].join('')
      Dir.mkdir path unless dir? path
    end
  end

  def rmf( fname )
    $stderr.puts "rm -f #{fname}" if @verbose
    return if @no_harm

    if File.exist? fname or File.symlink? fname then
      File.chmod 777, fname
      File.unlink fname
    end
  end

  def rmrf( dn )
    $stderr.puts "rm -rf #{dn}" if @verbose
    return if @no_harm

    Dir.chdir dn
    Dir.foreach('.') do |fn|
      next if fn == '.'
      next if fn == '..'
      if dir? fn then
        verbose_off {
          rmrf fn
        }
      else
        verbose_off {
          rmf fn
        }
      end
    end
    Dir.chdir '..'
    Dir.rmdir dn
  end

  def verbose_off
    save, @verbose = @verbose, false
    yield
    @verbose = save
  end

  def install( from, to, mode )
    $stderr.puts "install #{from} #{to}" if @verbose
    return if @no_harm

    if dir? to then
      to = to + '/' + File.basename(from)
    end
    str = nil
    File.open( from, 'rb' ) {|f| str = f.read }
    if diff? str, to then
      verbose_off {
        rmf to if File.exist? to
      }
      File.open( to, 'wb' ) {|f| f.write str }
      File.chmod mode, to
    end
  end

  def diff?( orig, comp )
    return true unless File.exist? comp
    s2 = nil
    File.open( comp, 'rb' ) {|f| s2 = f.read }
    orig != s2
  end

  def command( str )
    $stderr.puts str if @verbose
    system str or raise RuntimeError, "'system #{str}' failed"
  end

  def dir?( dname )
    # for CORRUPTED windows stat()
    File.directory?(dname[-1,1] == '/' ? dname : dname + '/')
  end

end


if $0 == __FILE__ then
  begin
    MainInstaller = Installer.new( ARGV )
    MainInstaller.execute
  rescue
    raise if $DEBUG
    $stderr.puts $!
    $stderr.puts 'try "ruby setup.rb --help" for usage'
    exit 1
  end
end
