# This is THE NACT configuration library module for perl programs
package NACT::config;
use strict;
use Sys::Syslog;
use Term::ScreenColor;
use Term::ReadLine;
use Term::ReadKey;
use Config;
use IO::Socket;
use IO::Interface		qw/:flags/;
use DBI;
use DBI::Const::GetInfoType;
use DBD::ODBC;
use DBD::SQLite;
use MLDBM;
use SQL::Statement;
use DBD::File;
use Text::CSV_XS;
use DBD::CSV;
use Config::General;

use Exporter;
use base 'Exporter';

use vars qw($_CENTER_ROW $_CENTER_COL $_ROWS $_COLS %_SIGNUM %_HOSTNICS %_NACTconfig $_SCR $_INTERACTIVE);
$_SCR = undef;
$_INTERACTIVE = 'NON-INTERACTIVE';

# ------------------------------------------------------------------------------

if ( -t STDIN && -t STDOUT )
{
	eval{ require Term::ScreenColor; };
	$_INTERACTIVE = ( @? ) ? 'NO-SCREENCOLOR' : 'INTERACTIVE';
}

# ------------------------------------------------------------------------------

if ($Config{sig_name} && $Config{sig_num})
{
	my @names = split ' ', $Config{sig_name};
	@_SIGNUM{@names} = split ' ', $Config{sig_num};
}

# ------------------------------------------------------------------------------

our $sock = IO::Socket::INET->new(Proto => 'udp');
%_HOSTNICS = map { $_ => $sock->if_addr($_) } $sock->if_list;

# ------------------------------------------------------------------------------

if ( -f "/usr/etc/NACT.conf" )
{
	if ( my $cfg = new Config::General('/usr/etc/NACT.conf') )
	{
		%_NACTconfig = $cfg->getall;
	}
}
elsif ( -f "./NACT.conf" )
{
	if ( my $cfg = new Config::General('./NACT.conf') )
	{
		%_NACTconfig = $cfg->getall;
	}
}

# ------------------------------------------------------------------------------

our @EXPORT = qw/%_NACTconfig @_interfaces $_SCR $_INTERACTIVE %_SIGNUM %_HOSTNICS/;

our @EXPORT_OK = qw/REFORMAT_HERE Syslogger GetKey GetString Echo InitTermScreen Commify
					CSVConnect Verbose_Syslogger SQLServerConnect SQLiteConnect ClearRecords ReadLn/;

# ==============================================================================

sub InitTermScreen
{
	return(1) if ( defined( $_SCR ) && ref($_SCR) =~ /^Term::Screen/ );
	unless( defined($_SCR) )
	{
		$_SCR = new Term::ScreenColor;
		$_SCR->clrscr();
		$_SCR->colorizable(1);
		$_ROWS = $_SCR->rows();
		$_COLS = $_SCR->cols();
		$_CENTER_COL = int($_COLS / 2);
		$_CENTER_ROW = int($_ROWS / 2);
		return 1;
	}
	else
	{
		return 0;
	}
}

# ==============================================================================

sub Echo
{
	my %p = (
				msg	=> "\n",
				clrscr	=> 0,
				clreol	=> 0,
				clrline	=> 0,
				cr 	=> 1,
				colors	=> 'reset',
				at	=> [],
				@_
	);

	InitTermScreen() unless ( defined($_SCR) );

	if ( ref($_SCR) =~ /^Term::Screen/ )
	{
		$_SCR->clrscr() if ( $p{clrscr} );
		my $blanks = chomp( $p{msg} );
		if ( $p{msg} && scalar( @{$p{at}} ) )
		{
			my $cols = 0;
			my $rows = $p{at}->[0];

			if ( $rows =~ /center/i )
			{
				$cols = int($_COLS/2); 
				$cols = $_CENTER_COL - int(length($p{msg})/2);
				$rows = $_CENTER_ROW;
			}
			elsif ( $rows =~ /bottom|status/i )
			{
				$rows = $_ROWS;
			}
			elsif ( $rows =~ /up(\d+)/i )
			{
				my $offset = $1;
				$rows = ($offset < $_ROWS) ? $_ROWS-$offset : 0;
			}
			elsif ( $rows =~ /down(\d+)/i )
			{
				my $offset = $1;
				$rows = ($offset < $_ROWS) ? $offset : $_ROWS;
			}

			if ( scalar( @{$p{at}} ) > 1 )
			{
				$cols = $p{at}->[1];
				if ( $p{at}->[1] =~ /center/i )
				{
					$cols = $_CENTER_COL - int(length($p{msg})/2);
				}
			}

			my $R = ($rows<0) ? 0 : $rows;
			my $C = ($cols<0) ? 0 : $cols;
			
			$_SCR->at($R, $C)->clreol() if ( $p{clreol} );
			$_SCR->at($R, 0)->clreol() if ( $p{clrline} );
			$_SCR->at($R, $C)->putcolored($p{colors}, $p{msg});
		}
		if ( $p{cr} )
		{
			for( 0 .. int(length($blanks)/2) ) { $_SCR->puts( "\n\r" ); };
		}
	}
	elsif( $_INTERACTIVE ne 'NON-INTERACTIVE' )
	{
		chomp( $p{msg} ) unless ( $p{cr} );
		print $p{msg};
	}
}

# ==============================================================================

sub GetKey
{
	my %p = (
				prompt	=> 'Press any key >> ',
				choices	=> '',
				default => '',
				at		=> [0, 0],
				@_,
	);

	my $key;
	my $default = '';
	my $ok = $p{choices};

	if ( $p{default} )
	{
		$default = ( split(//, $p{default}) )[0];
	}

	Echo( msg => $p{prompt}, colors => 'blue bold', at => $p{at}, cr => 0, clreol => 1 ) if ( $p{prompt} );

	$_SCR->timeout(0.1);
	READ_LOOP: while ( 1 )
	{
		$_SCR->flush_input();
		$key = $_SCR->noecho()->getch();
		my $val = ord($key);
		$key = $default if ( $default && $val == 13 );
		last READ_LOOP unless( $ok );
		last READ_LOOP if ( $ok =~ /\Q$key\E/ || $key eq $default );
	}

	return $key;
}

# ==============================================================================

sub GetString
{
	my %p = (
				prompt	=> ' >> ',
				colors	=> 'blue bold',
				at		=> [0, 0],
				@_,
	);

	my $key;
	my @str;
	my $default = '';

	Echo( msg => $p{prompt}, colors => $p{colors}, at => $p{at}, cr => 0, clreol => 1 ) if ( $p{prompt} );

	my $row = $p{at}->[0];
	my $col = $p{at}->[1] + length( $p{prompt} );

	READ_LOOP: while ( 1 )
	{
		$_SCR->flush_input();
		$key = $_SCR->echo()->getch();
		my $val = ord($key);
		next READ_LOOP if ( $val == 107 );
		last READ_LOOP if ( $val == 13 );

		if ( $val == 127 )
		{
			if ( scalar( @str ) )
			{
				pop(@str);
				--$col;
				$_SCR->at($row, $col)->puts(' ');
			}
			$_SCR->at($row, $col);
			next READ_LOOP;
		}
		++$col;
		push(@str, $key);
	}

	return( join('', @str) );
}

# ==============================================================================

sub ReadLn
{
	my %p = (
		prompt		=> 'NACT >> ',
		title		=> 'NACT MESSAGE',
		@_,
	);

	my $key;
	if ( $p{prompt} =~ /\[Yn\]/ || $p{prompt} =~ /\[nY\]/ )	# Yes/no - default "yes"
	{
		print $p{prompt};
		ReadMode 4;
		do {} until( defined( $key = ReadKey(-1) ) );
		ReadMode 0;
		print "\n";
		return ( $key eq 'n' || $key eq 'N' ) ? 0 : 1;		
	}
	elsif ( $p{prompt} =~ /\[yN\]/ || $p{prompt} =~ /\[Ny\]/ ) # Yes/no - default "no"
	{
		print $p{prompt};
		ReadMode 4;
		do {} until( defined( $key = ReadKey(-1) ) );
		ReadMode 0;
		print "\n";
		return ( $key eq 'y' || $key eq 'Y' ) ? 1 : 0;		
	}
	else
	{
		my $line;
		my $term = Term::ReadLine->new($p{title});
		if( defined( $line = $term->readline( $p{prompt} ) ) )
		{
			return ( $@ ) ? $@ : $line;
		}
		return( undef );
	}
}

# ==============================================================================

sub REFORMAT_HERE
{
	my $string = shift;
	$string =~ s/^\s+//gm;
	return $string;
}

# ==============================================================================

sub Syslogger
{	# Taken from Frank Price presentation, Lexington Perl users group
	my ($priority, $msg) = @_;
	return 0 unless ($priority =~ /info|err|debug/);
	# my ($package, $filename, $line) = caller;
	# setlogsock('unix');
	openlog($0, 'pid,cons', 'user');
	syslog($priority, $msg);
	closelog();
	return 1;
}
# ==============================================================================

sub Verbose_Syslogger
{	# Taken from Frank Price presentation, Lexington Perl users group
	my ($priority, $msg) = @_;
	return 0 unless ($priority =~ /info|err|debug/);
	my ($package, $filename, $line) = caller;
	# setlogsock('unix');
	openlog($0, 'pid,cons', 'user');
	syslog($priority, '<' . $filename . ', ' . $package . ', ' . $line . '>' . $msg);
	closelog();
	return 1;
}

# === Commify arrogantly stolen from the Perl Cookbook :O} =====================

sub Commify
{
    my $text = reverse $_[0];
    $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
    return scalar reverse $text;
}

# ==============================================================================

sub ClearRecords
{
	my %args = (
		DB		=> {},
		table	=> "all",
		@_,
	);

	if ( $args{table} eq "all" || $args{table} eq "capture" )
	{
		$args{DB}->{capture_record} = {
			tkey		=> "",
			src_pair	=> "",
			dst_pair	=> "",
			protocol	=> "",
			port		=> "",
			len			=> 0,
			caplen		=> 0,
			zipped		=> 0,
			packet		=> "",
			epoch		=> 0,
			segment		=> "",
			interface	=> ""
		}
	}
	
	if ( $args{table} eq "all" || $args{table} eq "hosts" )
	{
		$args{DB}->{hosts_record} = {
			ipaddr		=> "",
			hostname	=> "",
			segment		=> "",
			interface	=> ""
		}
	}

	if ( $args{table} eq "all" || $args{table} eq "conversants" )
	{
		$args{DB}->{conversants_record} = {
			src_pair		=> "",
			dst_pair		=> "",
			packet_count	=> 0,
			segment			=> "",
			interface		=> ""
		}	
	}
}

# ==============================================================================

sub SQLServerConnect
{
	my %args = (
		DB		=> {},
		test	=> 0,
		@_,
	);

	return 0 unless ( $_NACTconfig{DSN} =~ /ODBC/ );

	$args{DB} = {} unless ( ref($args{DB}) == 'HASH' );

	return 0 if ( exists($args{DB}->{dsn}) && $args{DB}->{dsn} !~ /ODBC/ );

	my ($user, $passwd, $dbname, $dsn, $msg);

	$user = ( exists($args{DB}->{user}) ) ? $args{DB}->{user} : $_NACTconfig{ODBC_USER};
	$passwd = ( exists($args{DB}->{passwd}) ) ? $args{DB}->{passwd} : $_NACTconfig{ODBC_PASSWD};
	$dbname = ( exists($args{DB}->{dbname}) ) ? $args{DB}->{dbname}
		: $_NACTconfig{ODBC_DATABASE_NAME} . " on " . $_NACTconfig{ODBC_SERVER_NAME};

	$dsn = ( exists($args{DB}->{dsn}) ) ? $args{DB}->{dsn} : $_NACTconfig{DSN};

	$msg = "SQL Server connection to " . $dbname;

	# --------------------------------------------------------------------------

	if ( my $dbh = DBI->connect($dsn, $user, $passwd, {RaiseError => 0, PrintError => 0, AutoCommit => 1}) )
	{
		$args{DB}->{version} = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
		$args{DB}->{dbms} = $dbh->get_info($GetInfoType{SQL_DBMS_NAME});

		if ( $args{test} )
		{
			$dbh->disconnect();
			return 1;
		}

		$args{DB}->{dbh} = $dbh;
		$args{DB}->{dsn} = $dsn;
		$args{DB}->{user} = $user;
		$args{DB}->{passwd} = $passwd;
		$args{DB}->{dbname} = $dbname;
		$args{DB}->{schema} = "dbadmin";
		$args{DB}->{msg} = $msg . ": OK";
	}
	else
	{
		$args{DB}->{msg} = $msg . " FAILED! -- $DBI::errstr";
		return 0;
	}

	# --------------------------------------------------------------------------

	ClearRecords( DB => $args{DB} );	
	
	# --------------------------------------------------------------------------

	@{$args{DB}->{capture_field_names}} = sort keys %{$args{DB}->{capture_record}};
	# @{$values} = @{$args{DB}->{capture_record}}{@{$args{DB}->{capture_field_names}}};

	$args{DB}->{capture_sql_insert} = "INSERT INTO " . $args{DB}->{schema} . ".capture ("
			. join(', ', @{$args{DB}->{capture_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{capture_field_names}})
			. ")"
		unless( exists( $args{DB}->{capture_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{hosts_field_names}} = sort keys %{$args{DB}->{hosts_record}};
	# @{$types} = @{$args{DB}->{hosts_record}}{@{$args{DB}->{hosts_field_names}}};

	$args{DB}->{hosts_sql_insert} = "INSERT INTO " . $args{DB}->{schema} . ".hosts ("
			. join(', ', @{$args{DB}->{hosts_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{hosts_field_names}})
			. ")"
		unless( exists( $args{DB}->{hosts_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{conversants_field_names}} = sort keys %{$args{DB}->{conversants_record}};
	# @{$types} = @{$args{DB}->{conversants_record}}{@{$args{DB}->{conversants_field_names}}};

	$args{DB}->{conversants_sql_insert} = "INSERT INTO " . $args{DB}->{schema} . ".conversants ("
			. join(', ', @{$args{DB}->{conversants_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{conversants_field_names}})
			. ")"
		unless( exists( $args{DB}->{conversants_sql_insert} ) );

	# --------------------------------------------------------------------------

	return 1 if ( $args{DB}->{capOK} && $args{DB}->{hostOK} && $args{DB}->{convOK} );
	
	# --------------------------------------------------------------------------

	my $sth;

	if ( $sth = $args{DB}->{dbh}->table_info("", "", "", "") )
	{
		if ( my $table = $sth->fetchall_hashref('TABLE_NAME') )
		{
			$args{DB}->{capOK} = ( exists($table->{capture}) ) ? 1 : 0;
			$args{DB}->{hostOK} = ( exists($table->{hosts}) ) ? 1 : 0;
			$args{DB}->{convOK} = ( exists($table->{conversants}) ) ? 1 : 0;
		}
	}
	else
	{
		$args{DB}->{msg} = "ODBC ERROR: unable to get database catalog information -- $DBI::errstr";
		return 0;
	}
	
	$sth->finish;
	
	unless ( $args{DB}->{capOK} )
	{
		my $sql = "CREATE TABLE " . $args{DB}->{schema} . ".capture ";
		$sql .= <<"		EOD";
		(
			tkey VARCHAR(50),
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			protocol VARCHAR(10),
			port VARCHAR(5),
			len INT,
			caplen INT,
			zipped TINYINT,
			packet VARCHAR(3000),
			epoch INT,
			segment VARCHAR(30),
			interface VARCHAR(4),
			record_id BIGINT
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "ODBC ERROR: unable to create table \"capture\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{hostOK} )
	{
		my $sql = "CREATE TABLE " . $args{DB}->{schema} . ".hosts ";
		$sql .= <<"		EOD";
		(
			ipaddr VARCHAR(30),
			hostname VARCHAR(40),
			segment VARCHAR(30),
			interface VARCHAR(4),
			record_id BIGINT
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "ODBC ERROR: unable to create table \"hosts\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{convOK} )
	{
		my $sql = "CREATE TABLE " . $args{DB}->{schema} . ".conversants ";
		$sql .= <<"		EOD";
		(
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			packet_count BIGINT,
			segment VARCHAR(30),
			interface VARCHAR(4),
			record_id BIGINT
		)
		EOD

		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "ODBC ERROR: unable to create table \"conversants\" -- " . $DBI::errstr;
			return 0;
		}
	}

	return 1;
}

# ==============================================================================

sub SQLiteConnect
{
	my %args = (
		DB		=> {},
		test	=> 0,
		@_,
	);

	my ($dsn, $dbname);

	$args{DB} = {} unless ( ref($args{DB}) == 'HASH' );
	
	$dbname = ( exists($args{DB}->{dbname}) ) ? $args{DB}->{dbname}
		: $_NACTconfig{CAPTURE_DIR} . '/' . $_NACTconfig{CAPDEVICE} . '.sdb';

	$dsn = ( exists($args{DB}->{dsn}) ) ? $args{DB}->{dsn} : "dbi:SQLite:dbname=" . $dbname;

	$args{DB}->{msg} = "SQLite connection to " . $dbname;

	# --------------------------------------------------------------------------

	if ( my $dbh = DBI->connect_cached($dsn, "", "", {RaiseError => 0, PrintError => 0, AutoCommit => 1}) )
	{
		$args{DB}->{version} = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
		$args{DB}->{dbms} = $dbh->get_info($GetInfoType{SQL_DBMS_NAME});
	
		$args{DB}->{msg} .= ": OK";
	
		if ( $args{test} )
		{
			$dbh->disconnect();
			return 1;
		}

		$args{DB}->{dbh} = $dbh;
		$args{DB}->{dbname} = $dbname;
		$args{DB}->{dsn} = $dsn;
	}
	else
	{
		$args{DB}->{msg} .= " FAILED! -- $DBI::errstr";
		return 0;
	}

	# --------------------------------------------------------------------------

	ClearRecords( DB => $args{DB} );	
	
	# --------------------------------------------------------------------------

	@{$args{DB}->{capture_field_names}} = sort keys %{$args{DB}->{capture_record}};
	# @{$types} = @{$args{DB}->{capture_record}}{@{$args{DB}->{capture_field_names}}};

	$args{DB}->{capture_sql_insert} = "INSERT INTO capture ("
			. join(', ', @{$args{DB}->{capture_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{capture_field_names}})
			. ")"
		unless( exists( $args{DB}->{capture_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{hosts_field_names}} = sort keys %{$args{DB}->{hosts_record}};
	# @{$types} = @{$args{DB}->{hosts_record}}{@{$args{DB}->{hosts_field_names}}};

	$args{DB}->{hosts_sql_insert} = "INSERT INTO hosts ("
			. join(', ', @{$args{DB}->{hosts_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{hosts_field_names}})
			. ")"
		unless( exists( $args{DB}->{hosts_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{conversants_field_names}} = sort keys %{$args{DB}->{conversants_record}};
	# @{$types} = @{$args{DB}->{conversants_record}}{@{$args{DB}->{conversants_field_names}}};

	$args{DB}->{conversants_sql_insert} = "INSERT INTO conversants ("
			. join(', ', @{$args{DB}->{conversants_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{conversants_field_names}})
			. ")"
		unless( exists( $args{DB}->{conversants_sql_insert} ) );

	# --------------------------------------------------------------------------

	return 1 if ( $args{DB}->{capOK} && $args{DB}->{hostOK} && $args{DB}->{convOK} );

	# --------------------------------------------------------------------------

	my $sth;
	
	unless( $sth = $args{DB}->{dbh}->prepare("SELECT name FROM SQLITE_MASTER WHERE type = 'table'") )
	{
		$args{DB}->{msg} = "SQLite3: $args{DB}I::errstr";
		return 0;
	}
	
	unless ( $sth->execute() )
	{
		$args{DB}->{msg} = "SQLite3: $args{DB}I::errstr";
		return 0;
	}
	
	if ( my $table = $sth->fetchall_hashref('name') )
	{
		$args{DB}->{capOK} = ( exists($table->{capture}) ) ? 1 : 0;
		$args{DB}->{hostOK} = ( exists($table->{hosts}) ) ? 1 : 0;
		$args{DB}->{convOK} = ( exists($table->{conversants}) ) ? 1 : 0;
	}
	else
	{
		$args{DB}->{msg} = "SQLite3: unable to get SQLite database catalog information -- $DBI::errstr";
		return 0;
	}

	$sth->finish;

	unless ( $args{DB}->{capOK} )
	{
		my $sql = "CREATE TABLE capture ";
		$sql .= <<"		EOD";
		(
			tkey VARCHAR(50),
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			protocol VARCHAR(10),
			port VARCHAR(5),
			len INTEGER,
			caplen INTEGER,
			zipped INTEGER,
			packet VARCHAR(3000),
			epoch INTEGER,
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "SQLite3: unable to create table \"capture\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{hostOK} )
	{
		my $sql = "CREATE TABLE hosts ";
		$sql .= <<"		EOD";
		(
			ipaddr VARCHAR(30),
			hostname VARCHAR(40),
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "SQLite3: unable to create table \"hosts\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{convOK} )
	{
		my $sql = "CREATE TABLE conversants ";
		$sql .= <<"		EOD";
		(
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			packet_count INTEGER,
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD

		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "SQLite3: unable to create table \"conversants\" -- " . $DBI::errstr;
			return 0;
		}
	}

	return 1;

}

# ==============================================================================

sub CSVConnect
{
	my %args = (
		DB		=> {},
		@_,
	);

	$args{DB} = {} unless ( ref($args{DB}) == 'HASH' );
	
	$args{DB}->{dbname} = $_NACTconfig{CAPTURE_DIR} . '/csv' unless( exists($args{DB}->{dbname}) );

	$args{DB}->{dsn} = "dbi:CSV:";

	$args{DB}->{msg} = "DBI-CSV connection: ";

	# --------------------------------------------------------------------------

	if ( $args{DB}->{dbh} = DBI->connect($args{DB}->{dsn}, undef, undef,
								{
									RaiseError	=> 0,
									PrintError	=> 0,
									f_dir		=> $args{DB}->{dbname},
									f_ext		=> ".csv",
									csv_class	=> "Text::CSV_XS",
								}
							)
		)
	{
		$args{DB}->{version} = $args{DB}->{dbh}->get_info($GetInfoType{SQL_DBMS_VER});
		$args{DB}->{dbms} = $args{DB}->{dbh}->get_info($GetInfoType{SQL_DBMS_NAME});
		
		$args{DB}->{msg} .= "OK -- using data directory: " . $args{DB}->{dbname};

		my @table_list = $args{DB}->{dbh}->func("list_tables");

		foreach my $table ( @table_list )
		{
			$args{DB}->{capOK} = 1 if ( $table =~ /capture/ );
			$args{DB}->{hostOK} = 1 if ( $table =~ /hosts/ );
			$args{DB}->{convOK} = 1 if ( $table =~ /conversants/ );
		}
	}
	else
	{
		$args{DB}->{msg} .= "FAILED -- " . $DBI::errstr;
		return 0;
	}

	# --------------------------------------------------------------------------

	ClearRecords( DB => $args{DB} );	
	
	# --------------------------------------------------------------------------

	@{$args{DB}->{capture_field_names}} = sort keys %{$args{DB}->{capture_record}};
	# @{$types} = @{$args{DB}->{capture_record}}{@{$args{DB}->{capture_field_names}}};

	$args{DB}->{capture_sql_insert} = "INSERT INTO capture ("
			. join(', ', @{$args{DB}->{capture_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{capture_field_names}})
			. ")"
		unless( exists( $args{DB}->{capture_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{hosts_field_names}} = sort keys %{$args{DB}->{hosts_record}};
	# @{$types} = @{$args{DB}->{hosts_record}}{@{$args{DB}->{hosts_field_names}}};

	$args{DB}->{hosts_sql_insert} = "INSERT INTO hosts ("
			. join(', ', @{$args{DB}->{hosts_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{hosts_field_names}})
			. ")"
		unless( exists( $args{DB}->{hosts_sql_insert} ) );

	# --------------------------------------------------------------------------

	@{$args{DB}->{conversants_field_names}} = sort keys %{$args{DB}->{conversants_record}};
	# @{$types} = @{$args{DB}->{conversants_record}}{@{$args{DB}->{conversants_field_names}}};

	$args{DB}->{conversants_sql_insert} = "INSERT INTO conversants ("
			. join(', ', @{$args{DB}->{conversants_field_names}})
			. ") VALUES ("
			. join(', ', ("?")x@{$args{DB}->{conversants_field_names}})
			. ")"
		unless( exists( $args{DB}->{conversants_sql_insert} ) );

	# --------------------------------------------------------------------------

	return 1 if ( $args{DB}->{capOK} && $args{DB}->{hostOK} && $args{DB}->{convOK} );

	# --------------------------------------------------------------------------

	unless ( $args{DB}->{capOK} )
	{
		my $sql = "CREATE TABLE capture ";
		$sql .= <<"		EOD";
		(
			tkey VARCHAR(50),
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			protocol VARCHAR(10),
			port VARCHAR(5),
			len INTEGER,
			caplen INTEGER,
			zipped INTEGER,
			packet VARCHAR(3000),
			epoch INTEGER,
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "CSV ERROR: unable to create table \"capture\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{hostOK} )
	{
		my $sql = "CREATE TABLE hosts ";
		$sql .= <<"		EOD";
		(
			ipaddr VARCHAR(30),
			hostname VARCHAR(40),
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD
	
		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "CSV ERROR: unable to create table \"hosts\" -- " . $DBI::errstr;
			return 0;
		}
	}

	unless ( $args{DB}->{convOK} )
	{
		my $sql = "CREATE TABLE conversants ";
		$sql .= <<"		EOD";
		(
			src_pair VARCHAR(30),
			dst_pair VARCHAR(30),
			packet_count INTEGER,
			segment VARCHAR(30),
			interface VARCHAR(4)
		)
		EOD

		unless( $args{DB}->{dbh}->do($sql) )
		{
			$args{DB}->{msg} = "CSV ERROR: unable to create table \"conversants\" -- " . $DBI::errstr;
			return 0;
		}
	}

	return 1;

}

# ==============================================================================

1;
