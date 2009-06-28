require 'rubygems'
$:.unshift File.dirname(__FILE__) + '/../lib'
require 'dbi'

class DbiBenchmark
	attr_reader :db

	def initialize db, quiet=false
		@db = db
		@quiet = quiet
	end

	def puts(*args)
		super unless @quiet
	end

	def run
		times = {}
		%w[create_test_table selecting_floats selecting_datetimes].each do |name|
			t = Time.now
			puts "* #{name.tr('_', ' ').capitalize}"
			send name
			took = Time.now - t
			puts "  (took #{took} seconds)"
			puts
			times[name] = took
		end
		times
	ensure
		db.do 'drop table data'
	end

	def create_test_table
		db.do <<-end
			create table data (
				date timestamp,
				value float
			)
		end
		db.do 'begin'
		today = Date.today
		5_000.times do
			db.do "insert into data values ('#{today + rand(100) - 50}', #{10 + rand * 30})"
		end
		db.do 'commit'
	end

	def selecting_floats
		strs = db.select_all('select value from data').map { |v| v.to_s }
		puts *strs[0, 5]
		puts '...'
	end

	def selecting_datetimes
		strs = db.select_all('select date from data').map { |v| v.to_s }
		puts *strs[0, 5]
		puts '...'
	end
end

def bench
	dbiurls = [
		'DBI:Mysql:dbitest:localhost',
		'DBI:ODBC:MYDBITEST',
	  'DBI:Pg:dbitest:localhost',
		'DBI:ODBC:PGDBITEST',
	]
	order = %w[create_test_table selecting_floats selecting_datetimes]
	dbiurls.map do |url|
	  # assume all dbs have the same credentials
		DBI.connect(url, *ARGV) do |db|
			[url.first.sub('DBI:', ''), *DbiBenchmark.new(db, true).run.values_at(*order)]
		end
	end
end

puts 'Running benchmark:'
DBI::Utils::TableFormatter.ascii(%w[db insert float datetime], bench, nil, nil, nil, nil, 30)

