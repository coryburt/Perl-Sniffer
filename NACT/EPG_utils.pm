#	This is the "EPG_utils" configuration library module for perl programs
#
#	By Cory Burt, contractor, Booz Allen Hamilton.
#               ___
#            .-9 9 `\
#          =(:(::)=  ;
#            ||||     \
#            ||||      `-.
#           ,\|\|         `,
#          /                \
#         ;                  `'---.,
#         |                         `\
#         ;                     /     |
#         \                    |      /
#          )           \  __,.--\    /
#       .-' \,..._\     \`   .-'  .-'
#      `-=``      `:    |   /-/-/`
#                   `.__/
#
#	@2011-2012, USAEPG under the auspices of the EPG contract administered
#	by ManTech and Partners, et.al.  All Rights Reserved.
# ------------------------------------------------------------------------------

package NACT::EPG_utils;

use Sys::Syslog;
use Config;
use IO::Socket;
use IO::Interface			qw/:flags/;
use DBI						qw/:sql_types/;
use DBI::Const::GetInfoType;
use MLDBM;
use SQL::Statement;
use SQL::Abstract;
use DBD::File;
use Text::CSV_XS;
use DBD::CSV;
use Config::General;
use File::Basename;
use Text::Wrapper;
use File::Find::Closures	qw/:all/;
use Log::Handler;
use Switch;
use Digest::HMAC_SHA1;
use Symbol 					qw/qualify_to_ref/;
use IO::Handle;
use IO::Select;

# use Term::ScreenColor;
# use Term::ReadLine;
# use Term::ReadKey;

use strict;

use Exporter;
use base 'Exporter';

use vars qw(%_SIGNUM %_HOSTNICS %_EPG_config $_INTERACTIVE %_SCR $_LOG $_HMAC);
$_LOG = undef;
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

if ( -f "/usr/etc/EPG_utils.conf" )
{
	if ( my $cfg = new Config::General(
							-ConfigFile			=> '/usr/etc/EPG_utils.conf',
							-InterPolateVars	=> 1 					)
	)
	{
		%_EPG_config = $cfg->getall;
		%{$_EPG_config{DSN}} = map { $_ => $_EPG_config{$_}{DEFAULT_DSN} } grep { /^DB_/ } keys %_EPG_config;
	}
}

# ------------------------------------------------------------------------------

our @EXPORT = qw/%_EPG_config @_interfaces $_INTERACTIVE %_SIGNUM %_HOSTNICS/;

our @EXPORT_OK = qw/Syslogger Commify Verbose_Syslogger ClrScr OpenDatabase
					ProgLog	ReadLn WriteLn PrintLn PadLn BaseName
					NiceSeconds Proggie ResetScr PacketDigest SysReadline/;

# ==============================================================================
# ==============================================================================
# --- "Private" functions ------------------------------------------------------
# ==============================================================================
# ==============================================================================

sub _Dasher
{
	my $str_ref = shift;
	my @msgs;

	foreach my $str ( @{$str_ref} )
	{
		chomp $str;
		if ( $str )
		{
			my $msg = ( length($str) > $_SCR{dashed_msg} ) ? substr( $str, 0, $_SCR{dashed_msg} ) : $str;
			$msg = ' [' . $msg . '] ';
			push(@msgs, $msg);
		}
	}

	my $dashed_line = sprintf( "%s", '-'x$_SCR{cols} );

	if ( scalar( @msgs ) )
	{
		my $final = join(' -- ', @msgs);
		$final = substr($final, 0, $_SCR{dashed_msg}) if ( length($final) > $_SCR{dashed_msg} );
		substr($dashed_line, 4, length($final), $final);
	}

	return( $dashed_line );
}

# ==============================================================================

sub _GetKey
{
	my $echo = shift || '';
	my $c = undef;

	$_SCR{obj}->noecho();
	while ( $c = $_SCR{obj}->getch() )
	{
		if ( $_SCR{obj}->key_pressed() )
		{
			$c = $_SCR{obj}->getch();
			last if ( 13 == ord($c) );
			$_SCR{obj}->flush_input();
			next;
		}
		print ( ( $echo ) ? $echo : $c );
		last;
	}
	$_SCR{obj}->echo();

	return $c;
}

# ==============================================================================

sub _GetString
{
	my $echo = shift || '';
	my $retval = '';

	while ( my $c = _GetKey($echo) )
	{
		last if ( 13 == ord($c) );
		$retval .= $c;
	}
	return $retval;
}

# ==============================================================================

sub _SqueezeString
{
	my ($input, $output_ref) = @_;
	chomp $input;
	my $wrapper = Text::Wrapper->new(columns => $_SCR{cols});

	my @tmp = split(/\n/, $input);

	foreach my $line ( @tmp )
	{
		my $big_line = $wrapper->($line);
		foreach my $subline ( split(/\n/, $big_line) )
		{
			push(@{$output_ref}, $subline);
		}
	}
}

# ==============================================================================
# --- Exported functions -------------------------------------------------------
# ==============================================================================

sub InitTermScreen
{
	my $retval = 0;
	if ( defined( $_SCR{obj} ) && ref( $_SCR{obj} ) =~ /^Term::Screen/ )
	{
		$_SCR{obj}->resize();
	}
	else
	{
		$_SCR{obj} = new Term::ScreenColor;
		$_SCR{obj}->colorizable(1);
		$retval = 1;
	}

	$_SCR{rows} = $_SCR{obj}->rows() - 1;
	$_SCR{cols} = $_SCR{obj}->cols() - 1;
	$_SCR{dashed_msg} = $_SCR{cols} - 9;

	return $retval;
}

sub ResetScr
{
	$_SCR{obj}->cooked();
}

# ==============================================================================

sub ClrScr
{
	InitTermScreen();
	$_SCR{obj}->clrscr()
		if ( defined( $_SCR{obj} ) && ref( $_SCR{obj} ) =~ /^Term::Screen/ );
}

# ==============================================================================

sub PrintLn
{
	my @inputs = @_;
	InitTermScreen();
	unless( scalar(@inputs) )
	{
		$_SCR{obj}->putcolored('reset', "\r\n");
		return;
	}

	my $msg = [];
	_SqueezeString($_, $msg) for ( @inputs );

	foreach my $line ( @{$msg} )
	{
		$_SCR{obj}->putcolored('reset', $line);
		$_SCR{obj}->putcolored('reset', "\r\n");
	}
}

# ==============================================================================

sub PadLn
{
	my @inputs = @_;
	InitTermScreen();
	my $outputs = [];

	foreach my $thing ( @inputs )
	{
		chomp $thing;
		my $msg;
		($msg = $thing) =~ s/\n/ /gsm;
		$msg =~ s/\r//gsm;
		push(@{$outputs}, $msg);
	}

	$_SCR{obj}->putcolored('reset', _Dasher($outputs));
	$_SCR{obj}->putcolored('reset', "\r\n");
}

# ==============================================================================

sub WriteLn
{
	my %args = (
				MSG			=> '',
				COLORS		=> 'reset',
				ROW			=> 0,
				COL			=> 0,
				PAD			=> 0,
				CLR_EOL		=> 0,
				@_
	);

	chomp $args{MSG};
	return unless( $args{MSG} );

	InitTermScreen();

	my ($len, $tot, $col, $row, $msg);

	if ( $args{ROW} > 0 )
	{
		$row = $args{ROW};
	}
	elsif( $args{ROW} < 0 )
	{
		$row = $_SCR{rows} + $args{ROW};
	}
	else
	{
		$row = $_SCR{rows};
	}

	$row = $_SCR{rows} if ($row>$_SCR{rows});
	$row = 0 if ($row<0);

	$col = $args{COL};
	$col = 0 if ($col<0);

	$len = length($args{MSG});
	$tot = $len + $col;
	while ( $tot > $_SCR{cols} && $col > 0 )
	{
		-- $col;
		$tot = $len + $col;
	}

	$msg = ( $tot > $_SCR{cols} ) ? substr($args{MSG},0,$_SCR{cols}) : $args{MSG};

	$msg = _Dasher([$msg]) if ( $args{PAD} );

	$_SCR{obj}->at($row, $col)->clreol() if( $args{CLR_EOL} );
	$_SCR{obj}->at($row, $col)->putcolored($args{COLORS}, $msg);
}

# === Ease the business of terminal interactivity ==============================

sub ReadLn
{
	my %args = (
		PROMPT	=> '[Yn] -->> ',
		COLORS	=> 'reset',
		ROW		=> 0,
		COL		=> 0,
		PASSWD	=> '',
		NL		=> 1,
		@_,
	);

	WriteLn( MSG => $args{PROMPT}, ROW => $args{ROW}, COL => $args{COL}, COLORS => $args{COLORS}, CLR_EOL => 1 );

	my $key;
	if ( $args{PROMPT} =~ /\[(\p{Alphabetic}+)\]/ )	# [...]
	{
		my $list = $1;
		my $default = ( $list =~ /^\p{LowercaseLetter}*(\p{UppercaseLetter}){1}\p{LowercaseLetter}*$/ ) ? $1 : undef;
		$list .= (defined($default)) ? lc($default) : '';

		while(1)
		{
			my $key = _GetKey();
			return $default if ( defined($default) && 13 == ord($key) );
			return $key if ( $list =~ /${key}/ );
		}
	}
	else
	{
		my $str = _GetString($args{PASSWD});
		print "\r\n" if ( $args{NL} );
		return( $str );
	}
}

# ==============================================================================

sub Proggie
{
	use Cwd qw/realpath/;
	my ($p, $d, $s) = fileparse($0, qr/\.[^.]*/);
	return( $p, realpath($d), $s);
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

# ==============================================================================

sub ProgLog
{
	my ($priority, $msg) = @_;
	return 0 unless( $msg );
	my ($package, $filename, $line) = caller;

	unless( ref( $_LOG ) eq 'Log::Handler' )
	{
		my ($p, $d, $s) = Proggie();
		$_LOG = Log::Handler->new(
			screen => {
				log_to		=> "STDOUT",
				maxlevel	=> "info",
				minlevel	=> "notice",
			},
			file => {
				filename	=> $d . '/' . $p . 'log',
				maxlevel	=> "notice",
				minlevel	=> "emergency",
				permissions	=> "0664",
			}
		);
	}

	switch ($priority)
	{
		case /info/		{ $_LOG->info( $msg ) }
		case /notice/	{ $_LOG->notice( $msg ) }
		case /warn/		{ $_LOG->warning( $msg ) }
		case /err/		{ $_LOG->error( "ERROR: " . $msg ) }
		case /crit/		{ $_LOG->critical( sprintf( ">> %s >> %s >> %s: %s", $package, $filename, $line, $msg ) ) }
		case /alert/	{ $_LOG->alert( sprintf( ">> %s >> %s >> %s: %s", $package, $filename, $line, $msg ) ) }
		case /emerg/	{ $_LOG->emergency( sprintf( ">> %s >> %s >> %s: %s", $package, $filename, $line, $msg ) ) }
		else			{ $_LOG->debug( sprintf( ">> %s >> %s >> %s: %s", $package, $filename, $line, $msg ) ) }
	};

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

sub OpenDatabase
{
	my %args = (
		TARGET		=> 'SQLSERVER',
		DB			=> {},
		TEST_ONLY	=> 0,
		LOG_ALL		=> 0,
		DEBUG		=> [],
		@_,
	);

	my $source = undef;
	my ($result, $dbh);
	my $sql_abstract = SQL::Abstract->new;
	my @tables;

	foreach my $config_dsn ( keys %{$_EPG_config{DSN}} )
	{
		if ( $config_dsn =~ /$args{TARGET}/i )
		{
			$source = $config_dsn;
			last;
		}
	}

	unless ( $dbh = DBI->connect($_EPG_config{DSN}->{$source}, {RaiseError => 0, PrintError => 0, AutoCommit => 1}) )
	{
		$result = "OpenDatabase connection to $source failed! $DBI::errstr";
		push( @{$args{DB}->{MSG}}, $result );
		push( @{$args{DEBUG}}, $result );
		Syslogger( 'err', $result ) if ( $args{LOG_ALL} );
		return 0;
	}
	elsif ( $args{TEST_ONLY} )
	{
		$dbh->disconnect;
		$result = "TEST DB-connection to $source succeeded!";
		push( @{$args{DB}->{MSG}}, $result);
		push( @{$args{DEBUG}}, $result );
		$args{DB}->{DBMS_VERSION} = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
		$args{DB}->{DBMS_NAME} = $dbh->get_info($GetInfoType{SQL_DBMS_NAME});
		Syslogger( 'info', $result ) if ( $args{LOG_ALL} );
		return $source;
	}

	unless( defined($source) )
	{
		my $result = "ERROR: no data source DSN was found to match \"$args{TARGET}!\"";
		Syslogger( 'err',  $result ) if ( $args{LOG_ALL} );
		push( @{$args{DB}->{MSG}}, $result );
		push( @{$args{DEBUG}}, $result );
		return 0;
	}

	if ( $source =~ /SQLSERVER/i )
	{
		require DBD::ODBC;
		my $catalog = '';
		my $schema = $_EPG_config{DB_SQLSERVER}{SCHEMA};
		$args{DB}->{SCHEMA} = $schema;
		my $schema_prefix = $schema . '.';
		my $name = '';
		my $column = '';

		if ( my $sth = $dbh->table_info($catalog, $schema, $name, 'TABLE') )
		{
			while( my $data = $sth->fetchrow_hashref )
			{
				push( @tables, $data->{TABLE_NAME} );
			}
			$sth->finish;
		}
		else
		{
			push( @{$args{DEBUG}}, $DBI::errstr );
		}

		foreach my $table ( @tables )
		{
			if ( my $sth = $dbh->column_info($catalog, $schema, $table, $column) )
			{
				my $rec = {};
				while ( my $row = $sth->fetchrow_hashref )
				{
					next if ( $row->{COLUMN_NAME} =~ /record_id/i );

					$rec->{$row->{COLUMN_NAME}} =
							($row->{TYPE_NAME} =~ /TEXT/i || $row->{TYPE_NAME} =~ /BLOB/i) ? '' : 0;
					push( @{ $args{DB}->{TABLES}->{$table}->{COLUMNS} }, $row->{COLUMN_NAME} );
				}
				$sth->finish;
				($args{DB}->{TABLES}->{$table}->{SQL_INSERT}, undef) = $sql_abstract->insert($schema_prefix . $table, $rec);
			}
			else
			{
				push( @{$args{DEBUG}}, $DBI::errstr );
			}
		}
	}
	else
	{
		require DBD::SQLite;
		my $sql = "SELECT sql FROM sqlite_master";
		my $metadata = $dbh->selectall_arrayref($sql, {Slice=>{}});

		foreach ( @{$metadata} )
		{
			if( $_->{sql} =~ /CREATE TABLE (\w+)/ )
			{
				my $table = $1;
				my $rec = {};
				if( $_->{sql} =~ /\((.*)\)/s )
				{
					my @cols = split(/,\n/, $1);
					foreach my $el ( @cols )
					{
						if ( $el =~ /^\s*(\w+)\s+(\w+)/)
						{
							my $col_name = $1;
							if ( $col_name !~ /record_id/ )
							{
								$rec->{$col_name} = $2;
								push( @{ $args{DB}->{TABLES}->{$table}->{COLUMNS} }, $col_name );
							}
						}
					}
					($args{DB}->{TABLES}->{$table}->{SQL_INSERT}, undef) = $sql_abstract->insert($table, $rec);
				}
			}
		}
	}

	$args{DB}->{DBMS_VERSION} = $dbh->get_info($GetInfoType{SQL_DBMS_VER});
	$args{DB}->{DBMS_NAME} = $dbh->get_info($GetInfoType{SQL_DBMS_NAME});

	$args{DB}->{DBH} = $dbh;		# Put the handle into the system global...

	return $source;
}

# ==============================================================================

sub NiceSeconds
{
	my $user_time = shift;
	my $secs = int($user_time);
	my $fractional = $user_time - $secs;

	my $mins = int($secs / 60);
	my $r_secs = $secs - ($mins*60);
	my $hours = int($mins / 60);
	my $r_mins = $mins - ($hours * 60);
	my $days = int($hours / 24);
	my $r_hours = $hours - ($days * 60);

	my $result = ($days) ? sprintf( "%d days, ", $days ) : '';
	$result .= ($hours) ? sprintf( "%d hours, ", $r_hours) : '';
	$result .= ($mins) ? sprintf( "%d minutes, ", $r_mins) : '';
	$result .= sprintf( "%.2f seconds", $r_secs + $fractional );

	return $result;
}

# ==============================================================================

sub _InitDigest
{
	if( ref($_HMAC) eq 'Digest::HMAC_SHA1' )
	{
		$_HMAC->reset;
		return;
	}
	my $hash = sprintf( "%-64.64s", $_EPG_config{DB_SQLSERVER}{DATABASE_NAME} . sprintf("%s", "0"x64) );
	$_HMAC = Digest::HMAC_SHA1->new($hash);
}

# ==============================================================================

sub PacketDigest
{
	my $data = shift;
	_InitDigest();
	$_HMAC->add($data);
	return( $_HMAC->b64digest );
}

# ==============================================================================

sub BaseName
{
	my ($b, undef, $s) = fileparse($_[0], qr/\.[^.]*/);
	return( $b.$s );
}

# ==============================================================================

sub SysReadline
{
    my($handle, $timeout) = @_;
    $handle = qualify_to_ref($handle, caller());
    my $infinitely_patient = (@_ == 1 || $timeout < 0);
    my $start_time = time();
    my $selector = IO::Select->new();
    $selector->add($handle);
    my $line = "";
SLEEP:
    until ( $line =~ /\n\z/ )
    {
        unless ($infinitely_patient)
        {
            return $line if time() > ($start_time + $timeout);
        }
        # sleep only 1 second before checking again
        next SLEEP unless $selector->can_read(1.0);
INPUT_READY:
        while ($selector->can_read(0.0))
        {
            my $was_blocking = $handle->blocking(0);
CHAR:       while (sysread($handle, my $nextbyte, 1))
            {
                $line .= $nextbyte;
                last CHAR if $nextbyte eq "\n";
            }
            $handle->blocking($was_blocking);
            # if incomplete line, keep trying
            next SLEEP unless( $line =~ /\n\z/ );
            last INPUT_READY;
        }
    }
    return $line;
}

# ==============================================================================

1;
