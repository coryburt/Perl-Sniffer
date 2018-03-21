#!/usr/bin/perl
#
#	capture2db (successor to packet2db 1.3)
#
#	This is the packet capture-to-db utility for the NACT
#	This program opens a "from-pipe" that from which it is passed raw packets.
#	It converts them to a form that can be kept in a Berkeley database.
#	It also dumps packets to a ring of no more than 100 pcap files of
#	approximately 100k each.
#
#	By Cory Burt
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
# ------------------------------------------------------------------------------

use NACT::config		qw/Syslogger Commify SQLServerConnect SQLiteConnect ClearRecords/;
use IO::Pipe;
use Storable			qw/store_fd fd_retrieve/;
use MIME::Base64		qw/encode_base64url/;
use DBI;
use DBD::SQLite;
use DB_File;
use Fcntl;
use Switch;
use NetPacket::Ethernet	qw/:types/;
use NetPacket::IP		qw/:protos/;
use NetPacket::ARP;
use NetPacket::TCP;
use Net::Pcap			qw/:datalink :functions/;
use Time::HiRes			qw/gettimeofday time/;
use Time::HiRes::Value;
use DateTime;
use Getopt::Long;
use File::Spec;
use POSIX				qw/mkfifo :signal_h/;
use Proc::Simple;
use CHI;
use FindBin				qw/$RealBin $Script $Bin/;

use constant	YES		=> 1;
use constant	NO		=> 0;
use constant	RING_BUFF_LIMIT	=> 99;
use constant	EUID	=> $>;

use strict;

# ------------------------------------------------------------------------------

unless( EUID == 0 )
{
	Syslogger( 'err', "[$Script] failed for lack of superuser privileges..." );
	print STDERR "$Bin/$Script aborted!  Superuser privileges required!\n";
	exit 1;
}

# ------------------------------------------------------------------------------

our $FORCE_DUMP_ROTATION = NO;
our $KEEP_GOING = YES;
our $debug = undef;
our $do_help;
our $FORK = 'Capture';
our ($capture, $err, %header, $packet);

our $stats	= {
				interface		=> '',
				sttime_str		=> NowInSeconds(),	# PCAP dump file start time
				local			=> NO,
				bytes_seen		=> 0,
				packets_seen	=> 0,
				bytes_saved		=> 0,
				packets_saved	=> 0,
};

# ------------------------------------------------------------------------------

if ( GetOptions(	"interface|i=s" => \$stats->{interface},
					"local"			=> \$stats->{local},
					"debug"			=> \$debug,
					"help|?|h"		=> \$do_help ) )
{
	Usage() if ( $do_help );

	Usage("$Script ABEND: the active interface command-line parameter is missing (e.g. \"--interface eth1\")")
		unless ( $stats->{interface} );

	Usage("$Script ABEND: \"$stats->{interface}\" does not appear to be an active interface on this host")
		unless ( exists( $NACT::config::_HOSTNICS{$stats->{interface}} ) );
}
else
{
	Usage("$Script ABEND: unrecognized command-line switch");
}

# ------------------------------------------------------------------------------
# --- Usage OK?  On we go.  First, open the interface for capturing... or die.
# ------------------------------------------------------------------------------

unless ( $capture = pcap_open_live($stats->{interface}, 65535, 1, 0, \$err) )
{
	Exit( "$Script ABEND: unable to open $stats->{interface} for capturing -- ($err)" );
}

# ==============================================================================

sub CloseWriter
{
	my $sig = shift;
	Syslogger( 'info', "[$Script] $FORK received $sig..." );
	print STDERR "$FORK received $sig...\n" if ( $NACT::config::_INTERACTIVE );
}

# ------------------------------------------------------------------------------

sub CloseCapture
{
	my $sig = shift;
	Syslogger( 'info', "[$Script] $FORK received $sig..." );
	print STDERR "\n$FORK received $sig...  " if ( $NACT::config::_INTERACTIVE && defined($debug) );
	$KEEP_GOING = NO;
}

# ------------------------------------------------------------------------------

sub ForceRotateDump
{
	$SIG{HUP} = \&ForceRotateDump;
	print STDERR "Reader rotating PCAP files...\n" if ( $NACT::config::_INTERACTIVE );
	$FORCE_DUMP_ROTATION = YES;
}

# ==============================================================================
# --- Now, get start-time, pcap file size, and instantiate FastMmap caches
# ==============================================================================

our %NACTconfig = %NACT::config::_NACTconfig;

our $exit_msg = "Capturing on " . $stats->{interface} . " closed by system signal";

our $CACHE_ROOT = $NACTconfig{CAPTURE_DIR};
our $DB_SHARE_FILE = $NACTconfig{CAPTURE_DIR} . $stats->{interface} . '_DB.dat';
our $PCAP_SHARE_FILE = $NACTconfig{CAPTURE_DIR} . $stats->{interface} . '_PCAP.dat';

our $DB_WRITER_OK = undef;
our $PCAP_WRITER_OK = undef;

unlink $DB_SHARE_FILE if( -e $DB_SHARE_FILE );
unlink $PCAP_SHARE_FILE if( -e $PCAP_SHARE_FILE );

our $db_cache = CHI->new(
	driver			=> 'FastMmap',
	root_dir		=> $CACHE_ROOT,
	share_file		=> $DB_SHARE_FILE,
	cache_size		=> '100k',
	init_file		=> 1,
	unlink_on_exit	=> 1,
);

our $pcap_cache = CHI->new(
	driver			=> 'FastMmap',
	root_dir		=> $CACHE_ROOT,
	share_file		=> $PCAP_SHARE_FILE,
	cache_size		=> '100k',
	init_file		=> 1,
	unlink_on_exit	=> 1,
);

# ------------------------------------------------------------------------------
# --- Start forking the "asynchronous" processes, but first
# --- make sure they all inherit the parent's interrupt handlers...
# ------------------------------------------------------------------------------

$SIG{HUP} = \&ForceRotateDump;
$SIG{TERM} = \&CloseCapture;

# ------------------------------------------------------------------------------
# --- Database writer process
# --- It opens and manages the databases, reads from the $DB_SHARE_FILE
# --- FastMmap cache and writes contents to the various database tables
# ------------------------------------------------------------------------------

our $db_writer = Proc::Simple->new();
our $DB_WRITER_STATUS = $db_writer->start( \&Database_Keeper, $CACHE_ROOT, $DB_SHARE_FILE );

# ------------------------------------------------------------------------------
# --- PCAP writer process
# --- It opens and writes to the PCAP ring file; reads from the
# --- $PCAP_SHARE_FILE FastMmap cache
# ------------------------------------------------------------------------------

our $pcap_writer = Proc::Simple->new();
our $PCAP_WRITER_STATUS = $pcap_writer->start( \&PCAP_Keeper, $CACHE_ROOT, $PCAP_SHARE_FILE );

# ------------------------------------------------------------------------------
# --- Capturing (parent) process continues here - it functions as the
# --- FastMmap cache data source for the three writers, (which get data to
# --- write from the caches, so they are actually cache readers -- sorry
# --- for the confusion that their naming may have caused in your head).
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# --- Wait, (a relatively LONG time), for children to finish initializing...
# ------------------------------------------------------------------------------
sleep 3;
# ==============================================================================
# --- Capture (broker) process main loop begins here...
# ==============================================================================

MAIN_LOOP: while ( pcap_next_ex( $capture, \%header, \$packet ) == 1 )
{
	my $tkey = sprintf( "%d|%d|%d|%d|%d",
		$header{tv_sec}, $header{tv_usec}, $header{len}, $header{caplen}, ++$stats->{packets_seen} );

	# --- Write the PCAP and NTOP FastMmap caches...
	$pcap_cache->set( $tkey, $packet );

	# --- Now, handle the database cache...
	my ($type, $ip_obj);
	my ($src, $dst, $port) = ('NULL', 'NULL', 'NULL');
	my $skip_db = NO;	# Assume the best...
	$stats->{bytes_seen} += $header{caplen};

	if( defined($debug) )
	{
		my $msg = sprintf( "Packet broker received packet of length : %d\n", $header{caplen} );
		print STDERR $msg;
	}

	my $eth_obj = NetPacket::Ethernet->decode($packet);

	my $smac = $eth_obj->{src_mac}  || 'NULL';
	my $dmac = $eth_obj->{dest_mac} || 'NULL';

	switch ( $eth_obj->{type} )
	{
		case ETH_TYPE_IP
		{
			$ip_obj = NetPacket::IP->decode($eth_obj->{data});
			switch ( $ip_obj->{proto} )
			{
				case IP_PROTO_IP		{ $type = "IP" }
				case IP_PROTO_ICMP		{ $type = "ICMP" }
				case IP_PROTO_IGMP		{ $type = "IGMP" }
				case IP_PROTO_IPIP		{ $type = "IPIP" }
				case IP_PROTO_TCP		{ $type = "TCP" }
				case IP_PROTO_UDP		{ $type = "UDP" }
				else					{ $type = "UNK" }
			}
			$src = $ip_obj->{src_ip}  || 'NULL';
			$dst = $ip_obj->{dest_ip} || 'NULL';

			$skip_db = YES if ( $src =~ /$NACTconfig{ADMIN_IP}/ );	# Filter packets to and from this host.  This is
			$skip_db = YES if ( $dst =~ /$NACTconfig{ADMIN_IP}/ );	# only meaningful if capturing on the admin interface
		}															# which would not normally be done in production.
		case ETH_TYPE_ARP
		{
			my $arp_obj = NetPacket::ARP->decode($eth_obj->{data});
			$type = "ARP";
			$smac = $arp_obj->{sha};
			$dmac = $arp_obj->{tha};
		}
		case ETH_TYPE_SNMP
		{
			$type = "SNMP";
		}
		else
		{
			$skip_db = YES	# ONLY RECOGNIZED ETH TYPES ARE SAVED IN THE DB!
		}
	}

	# ----------------------------------------------------------------------

	unless( $skip_db )
	{
		if ( $type == "TCP" )
		{
			my $tcp_obj = NetPacket::TCP->decode($ip_obj->{data});
			$port = $tcp_obj->{src_port} . '|' . $tcp_obj->{dest_port};
		}

		my $record = {
			src		=> $src,
			dst		=> $dst,
			src_mac => $smac,
			dst_mac => $dmac,
			type	=> $type,
			port	=> $port,
			len		=> $header{len},
			caplen	=> $header{caplen},
			zipped	=> 0,
			packet 	=> $packet,
		};

		$db_cache->set( $tkey, $record );
		# $db_cache->purge();

		$stats->{bytes_saved} += $header{caplen};
	}

	unless ( $db_writer->poll() )
	{
		$exit_msg = "[$Script] $FORK quitting because the database writer disappeared";
		$KEEP_GOING = NO;
	}

	unless ( $pcap_writer->poll() )
	{
		$exit_msg = "[$Script] $FORK quitting because the PCAP file writer disappeared";
		$KEEP_GOING = NO;
	}

	unless( $KEEP_GOING )
	{
		$db_writer->kill('SIGTERM');
		$pcap_writer->kill('SIGTERM');
		last MAIN_LOOP;
	}
}

Exit();

# ==============================================================================
# === End of main thread; function declarations begin here =====================
# ==============================================================================

sub Database_Keeper
{
	my ($root, $sharefile) = @_;

	$FORK="DB writer";

	my $DB = {};		# This MUST be initialized as a reference to an empty hash for database access to work...

	my ($capture, $transfers);
	my $retval = NO;

	# --- Open the capture database; SQLServer first, SQLite3 fallback ---------

	if ( $stats->{local} == NO && SQLServerConnect( DB => $DB ) )
	{
		Syslogger( 'info', "$Script: " . $DB->{msg} );
	}
	elsif ( SQLiteConnect( DB => $DB ) )
	{
		Syslogger( 'info', "$Script: " . $DB->{msg} );
	}
	else
	{
		Syslogger( 'err', "$Script:  " . $DB->{msg} );
		return( $retval );
	}

	# --- Now, prepare the statement handle for data capture -------------------

	unless( $capture = $DB->{dbh}->prepare($DB->{capture_sql_insert}) )
	{
		$DB->{dbh}->disconnect;
		Syslogger( 'err', "[$Script] died trying to prepare capture INSERT! -- $DBI::errstr" );
		return( $retval );
	}

	# --- Now that the open/prepare gauntlet has been passed... ----------------
	# --- open the FastMmap cache and begin writing to the DB ------------------

	my $cache = CHI->new(
		driver			=> 'FastMmap',
		root_dir		=> $root,
		share_file		=> $sharefile,
		cache_size		=> '100k',
	);
	
	# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

	OUTER_LOOP: while ( $KEEP_GOING )
	{
		my %hash = map { $_ => $cache->get($_) } $cache->get_keys();
		my @tkey_set = sort keys %hash;

		unless( scalar( @tkey_set ) )
		{
			sleep 1;
			next;
		}

		foreach my $tkey ( @tkey_set )
		{
			last OUTER_LOOP if( $tkey =~ /stop/i );

			ClearRecords( DB => $DB );

			my ($secs, $usecs, $len, $caplen, $cnt) = split( /\|/, $tkey );
			my $packet_epoch = sprintf( "%.0f", $secs . '.' . $usecs );

			$DB->{capture_record}->{tkey} = sprintf( "%011s|%011s|%09s", $secs, $usecs, $cnt);
			$DB->{capture_record}->{src_pair} = $hash{$tkey}->{src_mac}. '|' . $hash{$tkey}->{src};
			$DB->{capture_record}->{dst_pair} = $hash{$tkey}->{dst_mac}. '|' . $hash{$tkey}->{dst};
			$DB->{capture_record}->{protocol} = $hash{$tkey}->{type};
			$DB->{capture_record}->{port} = $hash{$tkey}->{port};
			$DB->{capture_record}->{len} = $len;
			$DB->{capture_record}->{caplen} = $caplen;

			if( $DB->{dbms} =~ /sqlite/i )
			{
				$DB->{capture_record}->{zipped} = 0;
				$DB->{capture_record}->{packet} = $hash{$tkey}->{packet};
			}
			else
			{
				$DB->{capture_record}->{zipped} = 1;
				$DB->{capture_record}->{packet} = encode_base64url( $hash{$tkey}->{packet} );
			}

			$DB->{capture_record}->{epoch} = $packet_epoch;
			$DB->{capture_record}->{segment} = $NACTconfig{SEGMENT_NAME};
			$DB->{capture_record}->{interface} = $NACTconfig{INTERFACE};

			my @values = @{$DB->{capture_record}}{@{$DB->{capture_field_names}}};

			Syslogger( 'err', "[$Script] DBI error inserting $tkey into capture table ($DBI::errstr)" )
				unless ( $capture->execute(@values) );
			$cache->remove( $tkey );
		}
	}

	# --------------------------------------------------------------------------

	$capture->finish;
	undef($capture);

	$DB->{dbh}->disconnect;
}

# ==============================================================================

sub PCAP_Keeper
{
	my ($root, $sharefile) = @_;

	$FORK = 'PCAP Writer';

	my $cache = CHI->new(
		driver			=> 'FastMmap',
		root_dir		=> $root,
		share_file		=> $sharefile,
		cache_size		=> '100k',
	);

	my ($dh, %ring);
	my $ring_cnt = 1;
	my ($s, undef) = NowString( $stats->{sttime_str} );

	# --------------------------------------------------------------------------

	my $pcap = pcap_open_dead( DLT_EN10MB, 65536 );
	my $current_dumpfile = sprintf ( "%s/%s_%s_D%s.pcap",
				$NACTconfig{CAPTURE_DIR}, $NACTconfig{SEGMENT_NAME}, $stats->{interface}, $s );
	$ring{$ring_cnt} = $current_dumpfile;
	unlink( $current_dumpfile ) if ( -e $current_dumpfile );

	my $OK = ( $dh = pcap_dump_open( $pcap, $current_dumpfile ) ) ? YES : NO;

	OUTER_LOOP: while ( $OK && $KEEP_GOING )
	{
		if ( $FORCE_DUMP_ROTATION )
		{
			pcap_dump_flush( $dh );
			pcap_dump_close( $dh );

			($s, undef) = NowString();
			$current_dumpfile = sprintf ( "%s%s_D%s.pcap", $NACTconfig{CAPTURE_DIR}, $stats->{interface}, $s );
			unlink( $current_dumpfile ) if ( -e $current_dumpfile );
			$dh = pcap_dump_open( $pcap, $current_dumpfile );

			++$ring_cnt;
			$ring_cnt = 1 if ( $ring_cnt == RING_BUFF_LIMIT );

			if ( exists( $ring{$ring_cnt} ) && -e $ring{$ring_cnt} )
			{
				unlink( $ring{$ring_cnt} );
			}

			$ring{$ring_cnt} = $current_dumpfile;
			$FORCE_DUMP_ROTATION = NO;
		}

		my %hash = map { $_ => $cache->get($_) } $cache->get_keys();
		my @tkey_set = sort keys %hash;

		unless( scalar( @tkey_set ) )
		{
			sleep 1;
			next OUTER_LOOP;
		}

		foreach my $tkey ( @tkey_set )
		{
			if( $tkey =~ /stop/i )
			{
				$OK = NO;
			}
			else
			{
				my %header;
				($header{tv_sec}, $header{tv_usec}, $header{len}, $header{caplen}, undef) = split( /\|/, $tkey );
				pcap_dump( $dh, \%header, $hash{$tkey} );
			}
			$cache->remove($tkey);
		}
		pcap_dump_flush( $dh );
		my @dumpstat = stat($current_dumpfile);
		if ( scalar(@dumpstat) )
		{
			$FORCE_DUMP_ROTATION = YES if ( $dumpstat[7] > 200_000_000 );
		}
	}

	if ( defined($dh) )
	{
		pcap_dump_close( $dh );
		Syslogger( 'info', "[$Script] $FORK closed file ring" );
	}
	else
	{
		Syslogger( 'info', "[$Script] $FORK -- file ring was not opened" );
	}
	Exit();
}

# ==============================================================================

sub NowString
{
	my $epoch = shift || NowInSeconds();
	my $machine = DateTime->from_epoch( epoch => $epoch );
	my $fd = $machine->ymd('_');
	my $sd = $machine->ymd('-');
	my $ft = $machine->hms('_');
	my $st = $machine->hms(':');
	return( $fd . '_T' . $ft, $sd . 'T' . $st );
}

# ==============================================================================

sub NowInSeconds
{
	return unless defined( wantarray );
	my $time_str = Time::HiRes::Value->now()->STRING();
	return( ( (wantarray) ? split(/\./, $time_str, 2) : $time_str ) );
}

# ==============================================================================

sub Exit
{
	my $msg = shift;

	if ( $msg )
	{
		Syslogger( 'err', "[$Script] -- $msg" );
		exit 1;
	}
	Syslogger( 'info', "[$Script] $FORK shutdown" );
	exit;
}

# ==============================================================================

sub Usage
{
	my $msg = shift;

	Exit($msg) unless ( $NACT::config::_INTERACTIVE );

	print "\nERROR: $msg\n" if ( $msg );

	my $blurb =<<"	EOBLURB";

	Usage:

	     $Script --interface <interface name> [--nact <NACT name>] [--local]

	or

	     $Script --help

	The "--interface" switch is required and must be followed by the name of the
	network interface being captured to the named-pipe.  For example, if the activity
	of the "eth1" interface is being captured, the switch would be:

	     --interface eth1

	The "--nact" switch is optional; it allows the operator to specify a
	unique identifier for the current NACT host.  This name becomes the
	fourth sub-string value of the key for every packet "$Script" saves
	to its database.  As such, this option is only useful in environments
	where more than one NACT is installed and databases might eventually be
	merged.  The default value is "PRI" which implies "PRIMARY."  It is
	recommened that this value be kept small -- similar to the 3-character
	default -- to help hold down database file sizes.
	
	The "--local" is also optional.  By specifying it, you would, effectively,
	force the program to capture to a local SQLite3 database instead of trying
	to connect to the MSSQL database that may (or may not) have been specified
	at install-time.

	NOTE! THIS PROGRAM IS NORMALLY STARTED AND STOPPED VIA THE daemontools
	PROCESS MANAGEMENT SYSTEM.  THAT MEANS THAT YOU SHOULD PROBABLY NOT BE
	STARTING OR STOPPING THIS PROGRAM DIRECTLY IN ANY PRODUCTION ENVIRONMENT.

	THE PROPER WAY TO START, STOP, AND MONITOR CAPTURING, IS BY SELECTING
	OPTIONS FROM THE console2 PROGRAM.  THIS PROGRAM WRAPS THE svc AND svstat
	PROGRAMS PROPERLY AND ENSURES CLEAN OPERATIONS.
	
	IN A PRODUCTION ENVIRONMENT, PLEASE USE	THE console2 PROGRAM TO MANAGE
	THIS CAPTURE SYSTEM!
	
	NOTE: Superuser privileges are required to run this program.  It will abort
	itself if it's effective user ID is non-zero.

	EOBLURB

	print $blurb, "\n";
	exit 1;
}

__END__

