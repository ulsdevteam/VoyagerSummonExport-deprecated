#!/m1/shared/bin/perl
=pod
=head1 NAME
VoyagerSummonExport : "Export MARC records from Voyager for Summon"
=head1 SYNOPSIS
	# An example of running this script serially

	my($exporter) = new VoyagerSummonExport(
		#  Use the Oracle username and password credentials that
		#  you use to run your Voyager Access canned reports.
		'oracle_username' => 'ro_mydb',
		'oracle_password' => '0bfu5cat3d',

		# The next two variables should ONLY be supplied values IF you are
		# installing this script on a NON-VOYAGER-DATABASE server and/or
		# the current (circa 2011) Voyager Direct (RITS) hosted setup.
		# Any server you install this script on MUST have have an Oracle
		# client, Perl, and the requisite Perl modules.
		'oracle_port' => '1521',
		'oracle_server' => 'localhost',

		#  To see what ORACLE_SID and ORACLE_HOME values you
		#  should use, run the "env" command while logged in
		#  to your Voyager server as the "voyager" user.
		'oracle_sid' => 'VGER',
		'oracle_home' => "/oracle/app/oracle/product/10.2.0.3/db_1",

		#  Your Voyager database
		'db_name' => 'mydb',

		# The location of the voyager reports directory
		# This will default to /m1/voyager/$db_name/rpt/ if not specified
		'report_directory' => '/m1/voyager/mydb/rpt/',

		# The location of the deleted files Archive
		# This will default to $report_directory or /m1/voyager/$db_name/rpt/ if not specified
		'archive_directory' => '/var/log/summon/rpt/',

		# Your summon username and password (and target FTP if needed)
		'summon_user' => 'mylib',
		'summon_password' => '0bfu5cat3d',
		'summon_site' => 'ftp.summon.serialssolutions.com',
		
		# Choose how you want to send holdings (default is none for bib-only)
		# none: don't send holdings
		# include: send holdings file seperately
		# 852-856: grab 852/856 fields from MFHD and insert into BIB
		'mfhd_option' => 'include',

		# it is safer to specify locations of external executables
		# it is optional if they are in your path and trusted
		'Pmarcexport' => '/m1/voyager/mydb/sbin/Pmarcexport',
		'ls' => '/bin/ls',
		'cp' => '/bin/cp',
		'cat' => '/bin/cat'
		'head' => '/bin/head'
		'tail' => '/bin/tail'
	);

	# minimally (and assuming the appropriate environment), the above could have been constructed as:
	$exporter = new VoyagerSummonExport( 
		'oracle_username' => 'ro_mydb',
		'oracle_password => '0bfu5cat3d', 
		'db_name' => 'mydb',
		'summon_user' => 'mylib',
		'summon_password' => '0bfu5cat3d',
		'archive_directory' => '/var/log/summon/rpt'
	);

	# archive the logs, getting last modification times
	my($previousTimestamp, $currentTimestamp) = $exporter->moveLogs();
	my($file);
	# export any changes since the last run
	$file = $exporter->exportChanges($previousTimestamp, $currentTimestamp)."\n";
	if ($exporter->is_error()) {
		warn join("\n", $exporter->error_report());
	} elsif ($file) {
		if (-s $file) {
			$exporter->sendChanges($file);
			warn join("\n", $exporter->error_report()) if ($exporter->is_error());
		}
	}

	# export any deletes since the last run
	$file = $exporter->exportDeletes($previousTimestamp, $currentTimestamp)."\n";
	if ($exporter->is_error()) {
		warn join("\n", $exporter->error_report());
	} elsif ($file) {
		if (-s $file) {
			$exporter->sendChanges($file);
			warn join("\n", $exporter->error_report()) if ($exporter->is_error());
		}
	}
	exit;


=head1 DESCRIPTION
Loosely based on a presentation by John Greer of University of Montana at 
ELUNA 2010: "Voyager to Summon". c.f. http://documents.el-una.org/506/

This script queries the Voyager database and checks deleted bibs/mfhds logs
to create MARC exports of changed (added, changed, deleted, suppressed)
records for export to the Summon discovery tool.

With the SQL queries, we are always looking at the latest data, but trying
to filter based on the created and modified dates.  Data integrity will be
lost as subsequent changes are made, but generally the script should be
accurate for re-doing recent exports.  The output files may not be identical
to what the output files were at a previous runtime, but they will not be
inaccurate with respect to the latest changes.
  
With the logs, we assume that we move the current logs to similar
filenames (perhaps in a different location) with a suffix of the 
date and time of the conclusion of the log. The command used to do such
is moveLogs().  Once this is done, it beyond the scope of this application
to undo.  If re-doing past exports, moveLogs() is not used.
It is these log archives, not the current log files, which are checked for
date validity and processed by exportChanges() and exportDeletes().

The general process is expected to be:

  moveLogs() :
    Move the current Voyager logs and touch new files in the log location.
    Wait for Voyager to stop writing to the inode, then return.

  exportChanges($beginning_datetime, $ending_datetime) :
    Query the database for BIBs and/or MFHDs created or updated within the
    datetime range.  Write the BIB ids to a tempfile.  Search the logs for
    deleted MFHDs suffixed with a datetimestamp within the datetime range.
    Extract the BIB ids from the deleted MFHDs and append these to the 
    tempfile.  Extract the MARC records for the BIBs represented in the 
    tempfile via Pmarcexport.  Postprocess the MARC records, manipulating 
    certain MARC fields (856, 852).  Return the names of the postprocessed
    file.

  sendChanges(@filenames) :
    Upload the changes files to Summon, storing or unlinking the original
    based on the presence or absense of the update_history_directory param.

  exportDeletes($beginning_datetime, $ending_datetime) :
    Query the database for BIBs suppressed within the datetime range and 
    export the MARC records via Pmarcexport to a tempfile.  Search the logs
    for deleted BIBs suffixed with a datetimestamp with the datetime range.
    Concatenate the tempfile with any matching deleted BIB logs.  Return the 
    names of the concatenated file.

  sendDeletes(@filenames) :
    Upload the deletes files to Summon, storing or unlinking the original
    based on the presence or absence of the delete_history_directory param.

=head1 DEBUGGING
    Set debug in the constructor to trace the process.
=cut

package VoyagerSummonExport;

use strict;
use DBI;
use MARC::Record;
use MARC::Batch;
use Encode;
use File::stat;
use Net::FTP;
use Net::SFTP;
use File::Copy;

use MARC::File::Encode;
# Override MARC::File::Encode::marc_to_utf8(), which was calling decode() with a CHECK param of 1.
# This CHECK croaked on the $batch->next()
{
	package MARC::File::Encode;
	no warnings 'redefine';
	*marc_to_utf8 = sub { 
		my($retVal);
		eval {
			# Original code from MARC::File::Encode::marc_to_utf8() dies on malformed UTF8
			$retVal = decode( 'UTF-8', $_[0], 1);
		} or do {
			# New code carps a message, but doesn't die
			$retVal = decode('UTF-8', $_[0]);
		};
		return $retVal;
	};
	use warnings 'redefine';
}

=pod
=head1 METHODS
=head2 new
Constructor, expects a hash with parameters

	my($exporter) = VoyagerSummonExport->new( %parameters );

=item debug
Optional, integer: turns on debugging (default off)

	debug => 0: off, none
	debug => 1: output trace messages to STDERR
	debug => 2: above, and add Pmarcexport messages and don't delete temp files
	debug => 3: above, and add SQL queries and merge of multiple MARC fields
	debug => 4: above, and add merge of any MARC fields

=item oracle_username
Required, string: The Oracle username for SQL connections

=item oracle_password
Required, string: The Oracle password for SQL connections

=item oracle_port
Optional, integer: The Oracle port for SQL connections.  Default is 1521.

=item oracle_server
Optional, string: The Oracle server name.  Default is localhost.

=item db_name
Required, string: The Oracle schema name.

=item oracle_sid
Required, string: The Oracle SID.  Default by pulling from ORACLE_SID in the environment.

=item oracle_home
Required, string: The path to the Oracle home directory.  Default by pulling from ORACLE_HOME in the environment.

=item summon_user
Required, string: The ftp/sftp username for Summon.

=item summon_password
Required, string: The ftp/sftp password for Summon.

=item summon_site
Optional, string: The DNS name of the Summon ftp/sftp site.  Default to 'ftp.summon.serialssolutions.com'.

=item use_sftp
Optional, boolean: Set this to use sftp instead of ftp.  We default to FTP because of a bug with mod_sftp/0.9.7 and Net::SFTP 0.10.

=item report_directory
Optional, string: The directory in which to find Voyager reports.  Defaults to '/m1/shared/$db_name/rpt/', where $db_name is the db_name parameter above.

=item history_directory
Optional, string: The directory in which to store update files and delete files which are sent to Summon.  If *history_directory directives are not specified, files are deleted after a successful send.  Trailing slash is optional.

=item update_history_directory
Optional, string: The directory in which to store update files which are sent to Summon.  Defaults to history_directory parameter.  If not specified, update files are deleted after a successful send.  Trailing slash is optional.

=item delete_history_directory
Optional, string: The directory in which to store delete files which are sent to Summon.  Defaults to history_directory parameter.  If not specified, delete files are deleted after a successful send.  Trailing slash is optional.

=item temp_directory
Optional, string: The directory for temporary file operations.  Defaults to '/tmp/'.  Trailing slash is optional.

=item Pmarcexport
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=item ls
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=item tail
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=item head
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=item cp
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=item cat
Optional, string: Enables specifying the location of the system command that will be called in the program.  If not specified, defaults to just the command name as found in the PATH.

=cut

sub new {
	# get object class reference
	my $class = shift;
	# everything else is a hash of parameters
	my %param = @_;
	# create a convenience variable for default for report_directory and archive_directory
	my($defaultDir);
	$defaultDir = '/m1/voyager/'.$param{'db_name'}.'/rpt/' if ($param{'db_name'});
	# create a constant that represents an unset variable
	my($unset) = 'VoyagerSummonExport::_unset_variable_';
	# define myself based on parameters
	my $self = {
		'debug' => $param{'debug'} || $unset,
		'oracle_username' => $param{'oracle_username'},
		'oracle_password' => $param{'oracle_password'},
		'oracle_port' => $param{'oracle_port'} || 1521,
		'oracle_server' => $param{'oracle_server'} || 'localhost',
		'db_name' => $param{'db_name'},
		'oracle_sid' => $param{'oracle_sid'} || $ENV{'ORACLE_SID'},
		'oracle_home' => $param{'oracle_home'} || $ENV{'ORACLE_HOME'},
		'summon_user' => $param{'summon_user'} || $unset,
		'summon_password' => $param{'summon_password'} || $unset,
		'summon_site' => $param{'summon_site'} || 'ftp.summon.serialssolutions.com',
		'Pmarcexport' => $param{'Pmarcexport'} || 'Pmarcexport',
		'report_directory' => $param{'report_directory'} || $defaultDir,
		'archive_directory' => $param{'archive_directory'} || $param{'report_directory'} || $defaultDir,
		'history_directory' => $param{'history_directory'} || $unset,
		'update_history_directory' => $param{'update_history_directory'} || $param{'history_directory'} || $unset,
		'delete_history_directory' => $param{'delete_history_directory'} || $param{'history_directory'} || $unset,
		'use_sftp' => $param{'use_sftp'} || $unset,
		'ls' => $param{'ls'} || 'ls',
		'tail' => $param{'tail'} || 'tail',
		'head' => $param{'head'} || 'head',
		'cp' => $param{'cp'} || 'cp',
		'cat' => $param{'cat'} || 'cat',
		'temp_directory' => $param{'temp_directory'} || '/tmp/'
	};
	# iterate over the keys and check if there are missing or correctable values
	my($k);
	foreach $k (keys(%$self)) {
		if (!$self->{$k}) {
			# warn if required parameter is missing
			my($e) = 'parameter "'.$k.'" is undefined';
			_error($self, $e);
			warn $e;
		} elsif ($k =~ m/_directory$/ && $self->{$k} ne $unset) {
			# add a trailing slash to directory parameters if missing
			$self->{$k} .= '/' unless ($self->{$k} =~ m'/$');
			unless ( -w $self->{$k} ) {
				# warn if directory parameter is invalid
				my($e) = 'directory "'.$k.'" is not writeable';
				_error($self, $e);
				warn $e;
			}
		}
	}
	foreach $k (keys(%param)) {
		if (!$self->{$k}) {
			warn 'parameter "'.$k.'" is unrecognized';
		}
	}
	# Unset any variables marked as such
	foreach $k (keys(%$self)) {
		if ($self->{$k} eq $unset) {
			undef $self->{$k};
		}
	}
	# return myself as an object
	return bless($self, $class);
}

=pod
=head2 lastTimespan
Retreives the timespan of the last known run.  Returns are the stat()->mtime() of the prior log moved, and of the last log.

	my($fromDatetime, $toDatetime) = $exporter->lastTimespan( );

=cut

sub lastTimespan {
	my $self = shift;
	my($cmd, $f);
	my($to, $from);
	# get the last deleted.bib.marc with a .extention
	$cmd = $self->{'ls'}.' -rt1 '.$self->{'archive_directory'}.'deleted.bib.marc.* | '.$self->{'tail'}.' -1';
	$f = `$cmd`;
	chomp $f;
	unless ($f) {
		$self->_error('Could not find last "deleted.bib.marc.*" in "'.$self->{'archive_directory'}.'"');
		return;
	}
	# the file modification time of this last deleted.bib.marc.YYYYMMDD.HHMM is the $to time
	$to = stat($f)->mtime();

	# get the deleted.bib.marc with a .extention before that
	$cmd = $self->{'ls'}.' -rt1 '.$self->{'archive_directory'}.'deleted.bib.marc.* | '.$self->{'tail'}.' -2 | '.$self->{'head'}.' -1';
	$f = `$cmd`;
	chomp $f;
	unless ($f) {
		$self->_error('Could not find second to last "deleted.bib.marc.*" in "'.$self->{'archive_directory'}.'"');
		return;
	}
	# the file modification time of this last deleted.bib.marc.YYYYMMDD.HHMM is the $from time
	$from = stat($f)->mtime();
	return $from, $to;
}

=pod
=head2 moveLogs
Moves the deleted logs to the archive location for use by this application.  Waits to return until it looks like the copied logs are stable (not currently being written).  Pass a true parameter to fake the move; defaults to false, which moves the active log.  Returns the datetime of the last log moved, and the datetime of the current stable log.

	my($previousLogDatetime, $currentLogDatetime) = $exporter->moveLogs( $doNotMove );

=cut

sub moveLogs {
	my $self = shift;
	my($doNotMove) = shift;
	my($filename);
	my(%filenames) = ('deleted.bib.marc' => '', 'delete.item' => '', 'deleted.mfhd.marc' => '');
	my(%filestats);
	my($timestamp) = $self->_log_date(time());
	# We'll be returning the datetime range in $from and $to, $cmd is a shell command, and $f is a target file
	my($from, $to, $cmd, $f);
	# get the last deleted.bib.marc with a .extention
	$cmd = $self->{'ls'}.' -rt1 '.$self->{'archive_directory'}.'deleted.bib.marc.* | '.$self->{'tail'}.' -1';
	$f = `$cmd`;
	chomp $f;
	unless ($f) {
		$self->_error('Could not find "deleted.bib.marc.*" in "'.$self->{'archive_directory'}.'"');
		return;
	}
	# the file modification time of this last deleted.bib.marc.YYYYMMDD.HHMM is the $from time
	$from = stat($f)->mtime();
	# Three files to keep in sync: deleted bibs delted mfhds, and deleted items
	# only deleted bibs and deleted mfhds are currently used
	foreach $filename (keys(%filenames)) {
		my($conflict) = 0;
		while (-e $self->{'archive_directory'}.$filename.'.'.$timestamp.($conflict ? '.'.$conflict : '')) {
			$conflict = ($conflict ? $conflict + 1 : 1);
		}
		if ($doNotMove) {
			$filenames{$filename} = $self->{'report_directory'}.$filename;
		} else {
			$filenames{$filename} = $self->{'archive_directory'}.$filename.'.'.$timestamp.($conflict ? '.'.$conflict : '');
			if (-e $self->{'report_directory'}.$filename) {
				# move the file from the active location to the archive
				if (move($self->{'report_directory'}.$filename, $filenames{$filename})) {
					# Touch the active log location
					my($u) = umask(0002);
					open(FH, '>>', $self->{'report_directory'}.$filename);
					close(FH);
					umask($u);
				} else {
					$self->_error('Unable to move "'.$self->{'report_directory'}.$filename.'" to "'.$filenames{$filename}.'"');
					return;
				}
			} else {
				# Create an empty file
				my($u) = umask(0002);
				open(FH, '>', $filenames{$filename});
				close(FH);
				umask($u);
			}
		}
		# collect the last modified time for each file
		$filestats{$filename} = stat($filenames{$filename})->mtime();
	}
	my($writing) = 1;
	# Sleep a bit and then compare the modified time for each file
	# If it's changed, wait a bit longer to allow any current writes to complete
	while ($writing) {
		sleep 5;
		$writing = 0;
		foreach $filename (keys(%filenames)) {
			$writing = 1 if ($filestats{$filename} != stat($filenames{$filename})->mtime());
		}
	}
	# The file modification time of the just moved deleted.bib.marc.YYYYMMDD.HHMM is the $to time
	$to = time();
	# Touch the logs to update date/time for the end of writing
	utime $to, $to, values(%filenames);
	return $from, $to;
}

=pod
=head2 exportDeletes
Creates a MARC export of records deleted.  Returns undef on error, or filename of the MARC export.  Takes parameters of $beginDate and $endDate as timestamps.

	my($bibDeleteExport) = $exporter->exportDeletes( $beginDate, $endDate );

=cut

sub exportDeletes {
	my $self = shift;
	# parameters:
	#   $beginDate: earliest date to search
	#   $endDate: latest date to search
	my ($beginDate, $endDate) = @_;
	if ($self->{'debug'}) {
		print 'DELETES: Collection from '.($beginDate ? $self->_oracle_date($beginDate) : 'earliest available date').' to '.($endDate ? $self->_oracle_date($endDate) : 'current time')."\n";
	}
	# Database handles
	#   $dbh is the database handle
	#   $sql is the text of an SQL statement
	#   $row is a resultset (hash, array)
	#   $sth is a DBI statement handle
	my ($dbh, $sql, $row, $sth);
	# $dbName is the convenience variable name of the database from the parameters
	my ($dbName) = $self->{'db_name'};
	# %SQLS is a hash of the base SQLs which may be limited to certain create/update dates
	# bib_master is checked for suppression
	# mfhd_master is checked for a degenerate case where the bib is unsuppressed, but no unsuppressed mfhds exist
	my(%SQLS) = (
		'bib_master' => {'base' => 'SELECT DISTINCT bib_master.bib_id FROM '.$dbName.'.bib_master, '.$dbName.'.bib_text WHERE bib_master.bib_id = bib_text.bib_id AND bib_master.suppress_in_opac = \'Y\' AND bib_text.network_number NOT LIKE \'(WaSeSS)%\''},
		'mfhd_master' => {'base' => 'SELECT DISTINCT bib_master.bib_id FROM '.$dbName.'.bib_master, '.$dbName.'.bib_mfhd, '.$dbName.'.mfhd_master, '.$dbName.'.bib_text WHERE bib_master.bib_id = bib_text.bib_id AND bib_master.suppress_in_opac = \'N\' AND bib_text.network_number NOT LIKE \'(WaSeSS)%\' AND bib_master.bib_id NOT IN ( SELECT b.bib_id FROM bib_mfhd b, mfhd_master m WHERE b.mfhd_id = m.mfhd_id AND m.suppress_in_opac = \'N\' ) AND bib_master.bib_id = bib_mfhd.bib_id AND bib_mfhd.mfhd_id = mfhd_master.mfhd_id AND mfhd_master.suppress_in_opac = \'Y\''}
	);
	# Add the update date limiters and union the SQLs as necessary
	$sql = '';
	if ($beginDate || $endDate) {
		# when beginDate or endDate is present, add an update_date clause to each table
		foreach $row (keys(%SQLS)) {
			# beginDate check for record dates after the specified date
			if ($beginDate) {
				$SQLS{$row}{'base'} .= ' AND '.$row.'.update_date > TO_DATE(\''.$self->_oracle_date($beginDate).'\', \'YYYY-MM-DD HH24:MI:SS\')';
			}
			# endDate check for record dates before or equal to the specified date
			if ($endDate) {
				$SQLS{$row}{'base'} .= ' AND '.$row.'.update_date <= TO_DATE(\''.$self->_oracle_date($endDate).'\', \'YYYY-MM-DD HH24:MI:SS\')';
			}
		}
	}
	# union the base sqls together
	foreach $row (keys(%SQLS)) {
		$sql .= ($sql ? ' UNION ' : '').$SQLS{$row}{'base'};
	}
	print 'sql :'.$sql."\n" if ($self->{'debug'} > 1);
	return unless ($dbh = $self->_getDBh());
	unless ($sth = $self->_getSTh($sql)) {
		$self->_releaseDBh();
		return;
	}
	unless ($self->_execSTh($sth)) {
		$self->_releaseDBh();
		return;
	}

	my(%filename) = (
		'bibids' => $self->{'temp_directory'}.'voyager-summon-export-delete-bibids.'.$$.'.txt',
		'marc' => $self->{'temp_directory'}.'voyager-summon-export-delete-data.'.$$.'.marc',
		'summon' => $self->{'temp_directory'}.$self->_summon_filename('deletes', ($endDate or time))
	);
	
	unless (open(OUTFILE,'>'.$filename{'bibids'})) {
		$self->_error($!);
		$self->_releaseDBh();
		return;
	}
	# Counter of rows fetched
	my($rowcount) = 0;
	while (($row) = $sth->fetchrow_array()) {
		$rowcount++;
		print OUTFILE $row."\n";
	}
	close(OUTFILE);
	$self->_releaseDBh();

	if ($rowcount) {
		print 'Requesting export of '.$rowcount.' records.  (see log.exp.'.$self->_log_date(time()).')'."\n" if ($self->{'debug'});
		#run Pmarcexport using the list of bib_ids pulled from voyager using the SQL above
		#the marc file created will be put in the marcfile variable set above
		# output to $filename{'marc'}, record type of Bibliographic, mode of MARC input file, target of $filename{'bibids'}
		$row = $self->{'Pmarcexport'}.($self->{'debug'} > 1 ? '' : ' -q').' -o '.$filename{'marc'}.' -rB -mM -t '.$filename{'bibids'}.($self->{'debug'} > 1 ? '' : ' > /dev/null');
		unless (system($row) == 0) {
			$self->_error($!.' when executing `'.$row.'`');
			$self->_releaseDBh();
			return;
		}

		# Check if Pmarcexport split the marc file
		if (!-e $filename{'marc'} && -e $filename{'marc'}.'.0') {
			# recombine it
			$row = $self->{'cat'}.' '.$filename{'marc'}.'.* >> '.$filename{'marc'};
			unless (system($row) == 0) {
				$self->_error($!.' when executing `'.$row.'`');
				$self->_releaseDBh();
				return;
			}
			unlink glob $filename{'marc'}.'.*' or warn $!.' when deleting '.$filename{'marc'}.'.*';
		}

		$row = $self->{'cp'}.' '.$filename{'marc'}.' '.$filename{'summon'};
		unless (system($row) == 0) {
			$self->_error($!.' when executing `'.$row.'`');
			$self->_releaseDBh();
			return;
		}
		unless ($self->{'debug'} > 1) {
			unlink $filename{'marc'} or warn $!.' when deleting '.$filename{'marc'};
		}
	}
	unless ($self->{'debug'} > 1) {
		unlink $filename{'bibids'} or warn $!.' when deleting '.$filename{'bibids'};
	}

	# Filenames of Voyager log files
	my(@filenames, $file);
	# TODO: handle file locations with spaces?!?
	# Gather the deleted BIBs.
	@filenames = glob($self->{'archive_directory'}.'deleted.bib.marc.*');
	foreach $file (@filenames) {
		# filename must be like our datestamped archives
		# filename's datestamp must fall between begin and end dates, if begin and end dates given
		my($beginMatch, $endMatch);
		$beginMatch = $self->_log_date($beginDate);
		$endMatch = $self->_log_date($endDate);
		if ($file =~ m/[0-9]{8}[.][0-9]{4}$/ && (($beginMatch && substr($file, -13) gt $beginMatch) || !$beginMatch) && (($endMatch && substr($file, -13) le $endMatch) || !$endMatch)) {
			print 'using logfile: '.$file."\n" if ($self->{'debug'});
			# include this file in the output
			if ($self->{'debug'}) {
				# quick count of MARC records
				$rowcount = 0;
				my($s) = $/;
				$/ = "\x1D";
				open(LOGFILE, $file);
				while (<LOGFILE>) {
					$rowcount++;
				}
				close(LOGFILE);
				$/ = $s;
				print 'Adding '.$rowcount.' records from log'."\n";
			}
			$row = $self->{'cat'}.' '.$file.' >> '.$filename{'summon'};
			unless (system($row) == 0) {
				$self->_error($!.' when executing `'.$row.'`');
				$self->_releaseDBh();
				return;
			}
		}
	}

	return $filename{'summon'};
}

=pod
=head2 exportChanges
Creates a MARC export of records changed.  Returns undef on error, or filename of the MARC export.  Takes parameters of $beginDate and $endDate as timestamps.

	my($bibChangeExport) = $exporter->exportChanges( $beginDate, $endDate );

=cut

sub exportChanges {
	my $self = shift;
	# parameters:
	#   $beginDate: earliest date to search
	#   $endDate: latest date to search
	my ($beginDate, $endDate) = @_;
	if ($self->{'debug'}) {
		print 'UPDATES: Collection from '.($beginDate ? $self->_oracle_date($beginDate) : 'earliest available date').' to '.($endDate ? $self->_oracle_date($endDate) : 'current time')."\n";
	}
	# Database handles
	#   $dbh is the database handle
	#   $sql is the text of an SQL statement
	#   $row is a resultset (hash, array)
	#   $sth is a DBI statement handle
	my ($dbh, $sql, $row, $sth);
	# $dbName is the convenience variable name of the database from the parameters
	my ($dbName) = $self->{'db_name'};
	# %SQLS is a hash of the base SQLs which may be limited to certain create/update dates
	# the base form is an unsuppressed BIB that is joined to a MFHD (the MFHD is not limited to unsuppressed because a change to suppress a MFHD is a change)
	$sql = 'SELECT DISTINCT bib_master.bib_id FROM '.$dbName.'.bib_master, '.$dbName.'.bib_mfhd, '.$dbName.'.mfhd_master, '.$dbName.'.bib_text WHERE bib_master.bib_id = bib_text.bib_id AND bib_master.bib_id = bib_mfhd.bib_id AND bib_mfhd.mfhd_id = mfhd_master.mfhd_id AND bib_master.suppress_in_opac = \'N\' AND bib_text.network_number NOT LIKE \'(WaSeSS)%\'';
	my(%SQLS) = (
		'bib_master' => {'base' => $sql },
		'mfhd_master' => {'base' => $sql }
	);
	# Add the create/update date limiters and union the SQLs as necessary
	$sql = '';
	if ($beginDate || $endDate) {
		# when beginDate or endDate is present, add a create_date and update_date clause to each table
		# @sqlUnions is a convenience variable to iterate create and update
		my(@sqlUnions) = ('create', 'update');
		foreach $row (keys(%SQLS)) {
			# first copy the SQL from base
			foreach $_ (@sqlUnions) {
				$SQLS{$row}{$_} = $SQLS{$row}{'base'};
			}
			# beginDate check for record dates after the specified date
			if ($beginDate) {
				foreach $_ (@sqlUnions) {
					$SQLS{$row}{$_} .= ' AND '.$row.'.'.$_.'_date > TO_DATE(\''.$self->_oracle_date($beginDate).'\', \'YYYY-MM-DD HH24:MI:SS\')';
				}
			}
			# endDate check for record dates before or equal to the specified date
			if ($endDate) {
				foreach $_ (@sqlUnions) {
					$SQLS{$row}{$_} .= ' AND '.$row.'.'.$_.'_date <= TO_DATE(\''.$self->_oracle_date($endDate).'\', \'YYYY-MM-DD HH24:MI:SS\')';
				}
			}
			# union these tables together
			foreach $_ (@sqlUnions) {
				$sql .= ($sql ? ' UNION ' : '').$SQLS{$row}{$_};
			}
		}
	} else {
		# when no begin or end date just union the base sqls together
		# this is a full export of the whole database!
		# Used SQLs tracks and prevents UNIONing duplicate SQLs
		my(%usedSQLs);
		foreach $row (keys(%SQLS)) {
			unless ($usedSQLs{$SQLS{$row}{'base'}}) {
				$sql .= ($sql ? ' UNION ' : '').$SQLS{$row}{'base'};
				$usedSQLs{$SQLS{$row}{'base'}} = 1;
			}
		}
	}
	print 'sql :'.$sql."\n" if ($self->{'debug'} > 1);
	return unless ($dbh = $self->_getDBh());
	unless ($sth = $self->_getSTh($sql)) {
		$self->_releaseDBh();
		return;
	}
	unless ($self->_execSTh($sth)) {
		$self->_releaseDBh();
		return;
	}

	my(%filename) = (
		'bibids' => $self->{'temp_directory'}.'voyager-summon-export-update-bibids.'.$$.'.txt',
		'marc' => $self->{'temp_directory'}.'voyager-summon-export-update-data.'.$$.'.marc',
		'summon' => $self->{'temp_directory'}.$self->_summon_filename($beginDate ? 'updates' : 'full', ($endDate or time))
	);

	unless (open(OUTFILE,'>'.$filename{'bibids'})) {
		$self->_error($!);
		$self->_releaseDBh();
		return;
	}
	# Counter of rows fetched
	my($rowcount) = 0;
	while (($row) = $sth->fetchrow_array()) {
		$rowcount++;
		print OUTFILE $row."\n";
	}
	print 'Query collected '.$rowcount.' records'."\n" if ($self->{'debug'});

	# A MARC Batch processor
	my($batch);
	# Filenames of Voyager log files
	my(@filenames, $file);
	# TODO: handle file locations with spaces?!?
	# Gather the deleted MFHDs.  These are changes.
	@filenames = glob($self->{'archive_directory'}.'deleted.mfhd.marc.*');
	foreach $file (@filenames) {
		# filename must be like our datestamped archives
		# filename's datestamp must fall between begin and end dates, if begin and end dates given
		my($beginMatch, $endMatch);
		$beginMatch = $self->_log_date($beginDate);
		$endMatch = $self->_log_date($endDate);
		if ($file =~ m/[0-9]{8}[.][0-9]{4}$/ && (($beginMatch && substr($file, -13) gt $beginMatch) || !$beginMatch) && (($endMatch && substr($file, -13) le $endMatch) || !$endMatch)) {
			print 'using logfile: '.$file."\n" if ($self->{'debug'});
			## get a MARC record from the MARC::Batch object.
			## the $record will be a MARC::Record object.
			$batch = MARC::Batch->new('USMARC', $file);
			$batch->strict_off();
			#why strict_off? The voyager records often have minor errors that I don't care about for this project
			## while loop will cycle through the marc file one record at a time
			my($logcount) = 0;
			while (my $record = $batch->next()) {
				$logcount++;
				$rowcount++;
				print OUTFILE $record->field('004')->as_string()."\n";
			}
			print 'Adding '.$logcount.' records from log'."\n" if ($self->{'debug'});
		}
	}
	
	close(OUTFILE);

	if ($rowcount) {
		print 'Requesting export of '.$rowcount.' records.  (see log.exp.'.$self->_log_date(time()).')'."\n" if ($self->{'debug'});
	} else {
		# No records to be exported: we're done.
		unless ($self->{'debug'} > 1) {
			unlink $filename{'bibids'} or warn $!.' when deleting '.$filename{'bibids'};
		}
		$self->_releaseDBh();
		return "";
	}

	#run Pmarcexport using the list of bib_ids pulled from voyager using the SQL above
	#the marc file created will be put in the marcfile variable set above
	# output to $filename{'marc'}, record type of Bibliographic, mode of MARC input file, target of $filename{'bibids'}
	$row = $self->{'Pmarcexport'}.($self->{'debug'} > 1 ? '' : ' -q').' -o '.$filename{'marc'}.' -rB -mM -t '.$filename{'bibids'}.($self->{'debug'} > 1 ? '' : ' > /dev/null');
	unless (system($row) == 0) {
		$self->_error($!.' when executing `'.$row.'`');
		$self->_releaseDBh();
		return;
	}

	# Check if Pmarcexport split the marc file
	if (!-e $filename{'marc'} && -e $filename{'marc'}.'.0') {
		# recombine it
		$row = $self->{'cat'}.' '.$filename{'marc'}.'.* >> '.$filename{'marc'};
		unless (system($row) == 0) {
			$self->_error($!.' when executing `'.$row.'`');
			$self->_releaseDBh();
			return;
		}
		unlink glob $filename{'marc'}.'.*' or warn $!.' when deleting '.$filename{'marc'}.'.*';
	}

	## get a MARC record from the MARC::Batch object.
	## the $record will be a MARC::Record object.
	$batch = MARC::Batch->new('USMARC', $filename{'marc'});
	$batch->strict_off();
	#why strict_off? The voyager records often have minor errors that I don't care about for this project

	#create an extract file that we will write to using the contents of the $marcfile
	#and the results of the data massaging below
	unless (open(OUTFILE,'>'.$filename{'summon'})) {
		$self->_error($!);
		$self->_releaseDBh();
		return;
	}
	# this select pulls the holding records for each bibliographic record
	# TODO: check whether it is faster to use $dbName.getMFHDBlob(bib_mfhd.mfhd_id) or $dbName.getMarcField(...)
	$sql = 'SELECT mfhd_data.mfhd_id, mfhd_data.seqnum, mfhd_data.record_segment FROM '.$dbName.'.mfhd_data JOIN '.$dbName.'.bib_mfhd ON (mfhd_data.mfhd_id = bib_mfhd.mfhd_id) JOIN '.$dbName.'.mfhd_master ON (mfhd_master.mfhd_id = bib_mfhd.mfhd_id) WHERE bib_mfhd.bib_id = ? AND mfhd_master.suppress_in_opac = \'N\' ORDER BY mfhd_data.mfhd_id, mfhd_data.seqnum';
	unless ($sth = $self->_getSTh($sql)) {
		$self->_releaseDBh();
		return;
	}

	## while loop will cycle through the marc file one record at a time
	$rowcount = 0;
	while (my $record = $batch->next()) {
		$rowcount++;

		unless ($sth->execute($record->field('001')->as_string())) {
			$self->releaseDBh();
			return;
		}

		my(@efields, $field);
		my($marctext) = '';
		my($holdingId, $lastHoldingId) = (0, 0);
		# Check for existing fields in BIB
		if (@efields = $self->_fields_from_marc($record)) {
			# if found, remove them and queue them for re-adding
			# this forces strict sort order, even though no required by MARC format
			$record->delete_fields(@efields);
			# throw away any 852 from the bib
			my(@tmpFields, $tmpField);
			foreach $tmpField (@efields) {
				if ($tmpField->tag() != 852) {
					push @tmpFields, $tmpField;	
				}
			}
			@efields = @tmpFields;
		}
		# Pull the MARC data into $marctext, $row by $row
		while (($holdingId, undef, $row) = $sth->fetchrow_array()) {
			# check for required fields if a new $holdingId
			if ($marctext && $holdingId != $lastHoldingId) {
				push @efields, $self->_fields_from_marc_text($marctext);
				$marctext = '';
			}
			$lastHoldingId = $holdingId;
			$marctext .= $row;
		}

		# only process the record if at least one holding was found
		if ($lastHoldingId) {
			# check for required fields on last MARC
			if ($marctext) {
				push @efields, $self->_fields_from_marc_text($marctext);
			}

			if (@efields) {
				print 'adding '.@efields.' fields to '.$record->field('001')->as_string()."\n" if (@efields > 1 && $self->{'debug'} > 2);
				print 'adding '.@efields.' fields to '.$record->field('001')->as_string()."\n" if (@efields == 1 && $self->{'debug'} > 3);
				$record->insert_fields_ordered(@efields);
			}

			## catch any errors and send to display, this will show the non validated MARC but still allow the program to run
			if ( my @warnings = $batch->warnings() ) {
				print STDERR "\n".'NOTICE: bib_id #'.$record->field('001')->as_string().' needs manual attention'."\n\n";
			}
			
			#Write out the full marc record from $marcfile to the OUTFILE defined above, including appended fields
			print OUTFILE encode_utf8( $record->as_usmarc() );
		} else {
			# Verify whether this BIB is unsuppressed (it could have come from the deleted MFHDs).
			# If unsuppressed, warn about missing MFHDs.
			my($tmpSth);
			if ($tmpSth = $self->_getSTh('SELECT bib_master.bib_id FROM '.$dbName.'.bib_master WHERE bib_master.bib_id = ? and bib_master.suppress_in_opac = \'N\'')) {
				if ($tmpSth->execute($record->field('001')->as_string())) {
					if ($tmpSth->fetchrow_array()) {
						print STDERR 'NOTICE: Unsuppressed BIB '.$record->field('001')->as_string().' has no valid holdings'."\n\n" if ($self->{'debug'});
					}
				}
			}
		}
	}

	print 'Merged location information for '.$rowcount.' records'."\n" if ($self->{'debug'});

	close(OUTFILE);

	$self->_releaseDBh();

	unless ($self->{'debug'} > 1) {
		unlink $filename{'bibids'} or warn $!.' when deleting '.$filename{'bibids'};
		unlink $filename{'marc'} or warn $!.' when deleting '.$filename{'marc'};
	}
	return $filename{'summon'};

}

=pod
=head2 sendDeletes
Uploads a MARC file to the Summon /deletes/ folder.  Required parameter is the MARC filename.  Returns undef on error, or true on success. SIDE EFFECT: if delete_history_directory is set, will move the file there; otherwise, the file will be deleted.

	my($success) = $exporter->sendDeletes( $filename );

=cut

sub sendDeletes {
	my $self = shift;
	my($f) = @_;
	return $self->_send_file($f, 0);
}

=pod
=head2 sendChanges
Uploads a MARC file to the Summon /updates/ folder. Required parameter is the MARC filename. Returns undef on error, or true on success. SIDE EFFECT: if update_history_directory is set, will move the file there; otherwise, the file will be deleted

	my($success) = $exporter->sendChanges( $filename );

=cut
	
sub sendChanges {
	my $self = shift;
	my($f) = @_;
	return $self->_send_file($f, 1);
}

=pod
=head2 sendFull
Uploads a MARC file to the Summon /full/ folder.  Required parameter is the MARC filename.  Returns undef on error, or true on success.  SIDE EFFECT: if update_history_directory is set, will move the file there; otherwise, the file will be deleted

	my($success) = $exporter->sendFull( $filename );

=cut

sub sendFull {
	my $self = shift;
	my($f) = @_;
	return $self->_send_file($f, 2);
}

=pod
=head2 is_error
Check for error. Returns false if no error, true if error occurred

	my($errorOccurred) = $exporter->is_error( );

=cut

sub is_error {
	my $self = shift;
	# just return true/false on whether the error content is set
	return $self->{'error'} ? 1 : 0;
}

=pod
=head2 error_report
Report and clear last error message.  Returns an array of error messages.

	my(@errorList) = $exporter->error_report( );
=cut

sub error_report {
	my $self = shift;
	# check for error content
	if ($self->{'error'}) {
		# return and clear the error
		my (@err) = @{$self->{'error'}};
		delete $self->{'error'};
		return @err;
	}
	# no error, return false
	return 0;
}

# take a timestamp and format as an Oracle string
sub _oracle_date {
	my $self = shift;
	my ($time) = @_;
	my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
	$mon+=1;
	$year+=1900;
	return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $year, $mon, $mday, $hour, $min, $sec);
}

# take a timestamp and format as an log date/timestamp
sub _log_date {
	my $self = shift;
	my ($time) = @_;
	my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst);
	($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
	$mon+=1;
	$year+=1900;
	return sprintf('%04d%02d%02d.%02d%02d', $year, $mon, $mday, $hour, $min);
}

# Uploads a MARC file to the Summon via preferred protocol
# required parameter is the MARC filename and type of upload (0 = delete, 1 = update, 2 = full)
# returns undef on error, or true on success
# SIDE EFFECT: if delete_history_directory / update_history_directory is set, will move the file there;
#    otherwise, the file will be deleted
sub _send_file {
	my $self = shift;
	my($filename, $type) = @_;

	if (! -e $filename ) {
		$self->_error('file '.$filename.' is missing');
		return;
	}

	# We have set this to default to ftp rather than sftp because Serials Solutions is currently using mod_sftp/0.9.7.
	# This server software is known to be incompatible with Net::SFTP 0.10 (c.f Known Client Issues: http://www.proftpd.org/docs/contrib/mod_sftp.html)
	# Symptoms are a hang on Net::SFTP->new() at KEXINIT
	if (($self->{'use_sftp'} && $self->_sftp_file($filename, $type)) || $self->_ftp_file($filename, $type)) {
		if ($self->{($type ? 'update' : 'delete').'_history_directory'}) {
			my($f) = $filename;
			$f =~ s'.*/'';
			$f = $self->{($type ? 'update' : 'delete').'_history_directory'}.$f;
			unless (move($filename, $f)) {
				$self->_error('Failed to move "'.$filename.'" to "'.$f);
				return;
			}
		} else {
			unlink $filename or warn $!.' when deleting '.$filename;
		}
	} else {
		return;
	}
	return 1;
}

# Uploads a MARC file to the Summon via SFTP
# required parameter is the MARC filename and type of upload (0 = delete, 1 = update, 2 = full)
# returns undef on error, or true on success
sub _sftp_file {
	my $self = shift;
	my($filename, $type) = @_;

	my($ftp) = Net::SFTP->new($self->{'summon_site'}, ('user' => $self->{'summon_user'}, 'password' => $self->{'summon_password'}));
	if ($ftp) {
		my($basename) = $filename;
		$basename =~ s'.*/'';
		unless ($ftp->put( $filename, ($type ? ($type == 2 ? 'full' : 'updates') : 'deletes').'/'.$basename ) ) {
			$self->_error('SFTP put: '.$ftp->status);
			undef $ftp;
			return;
		}
		undef $ftp;
	} else {
		$self->_error($@);
		return;
	}
	return 1;
}

# Uploads a MARC file to the Summon via FTP
# required parameter is the MARC filename and type of upload (0 = delete, 1 = update, 2 = full)
# returns undef on error, or true on success
sub _ftp_file {
	my $self = shift;
	my($filename, $type) = @_;

	my($ftp) = Net::FTP->new($self->{'summon_site'});
	if ($ftp) {
		unless ($ftp->login($self->{'summon_user'}, $self->{'summon_password'})) {
			$self->_error('FTP login: '.$ftp->message);
			return;
		}
		unless ($ftp->cwd( ($type ? ($type == 2 ? '/full' : '/updates') : '/deletes') )) {
			$self->_error('FTP cwd: '.$ftp->message);
			return;
		}
		unless ($ftp->put( $filename ) ) {
			$self->_error('FTP put: '.$ftp->message);
			return;
		}
		$ftp->quit;
	} else {
		$self->_error($@);
		return;
	}
	return 1;
}

# Wraps _fields_from_marc so we can call it with MARC text instead of a MARC::Record
sub _fields_from_marc_text {
	my $self = shift;
	my ($marcText) = @_;
	my($marc) = MARC::Record->new_from_usmarc($marcText);
	return $self->_fields_from_marc($marc);
}

# Returns particular fields (852, 856) from a MARC::Record
sub _fields_from_marc {
	my $self = shift;
	my($marc) = @_;
	my(@fields, $field, $fieldKey);
	foreach $fieldKey ('852', '856') {
		if ($field = $marc->field($fieldKey)) {
			push @fields, $field;
		}
	}
	return @fields;
}

# Create a Summon formatted MARC file name
sub _summon_filename {
	# See formatting convention per: https://proquestsupport.force.com/portal/apex/homepage?id=kA0400000004J6wCAE&l=en_US
	my $self = shift;
	my($format, $time) = @_;
	my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
	$mon=$mon+1;
	$year=$year+1900;
	return $self->{'summon_user'}.'-'.$format.'-'.sprintf('%02d-%02d-%02d-%02d-%02d', $year, $mon, $mday, $hour, $min).'.marc';
}

# Create a Database Handle
sub _getDBh {
	my $self = shift;
	# Just return the existing DB handle if present
	if ($self->{'dbh'}) {
		$self->{'dbh_counter'}++;
		return $self->{'dbh'};
	}

	# Ensure certain environment variables are set
	foreach my $k ('oracle_sid', 'oracle_home') {
		if (!$ENV{uc($k)} && $self->{$k}) {
			$ENV{uc($k)} = $self->{$k};
		}
	}

	# Create a new DB handle
	my $oracle_host_info = '';
	if ($self->{'oracle_server'} && $self->{'oracle_sid'}) {
		$oracle_host_info = 'host='.$self->{'oracle_server'}.';SID='.$self->{'oracle_sid'};
		if ($self->{'oracle_port'}) {
			$oracle_host_info .= ';port='.$self->{'oracle_port'};
		}
	}
	my $dbh;
	$dbh = DBI->connect('dbi:Oracle:'.$oracle_host_info, $self->{'oracle_username'}, $self->{'oracle_password'});
	# log error if connect fails
	unless ($dbh) {
		$self->_error('DBI->connect() failed: '.$DBI::errstr);
		return;
	}
	$self->{'dbh_counter'} = 1;
	$self->{'dbh'} = $dbh;
	return $dbh;
}

# Disconnect from the database
sub _releaseDBh {
	my $self = shift;
	if ($self->{'dbh_counter'} > 1) {
		$self->{'dbh_counter'}--;
	} elsif ($self->{'dbh'}) {
		$self->{'dbh'}->disconnect();
		undef($self->{'dbh'});
	}
	return 1;
}

# Convenience function to create a Statement Handle
# Mostly, this avoids repeating the error checking/reporting code
sub _getSTh {
	my $self = shift;
	my $sql = shift;
	my $sth;
	unless ($self->{'dbh'}) {
		$self->_error('Database not connected.');
		return;	
	}
	unless ($sql) {
		$self->_error('SQL not passed.');
		return;	
	}
	unless ($sth = $self->{'dbh'}->prepare($sql)) {
		$self->_error('dbh->prepare failed: '.$self->{'dbh'}->errstr());
		return;
	}
	return $sth;
}

# Convenience function to execute a Statement Handle with parameters
# Mostly, this avoids repeating the error checking/reporting code
sub _execSTh {
	my $self = shift;
	my $sth = shift;
	unless ($sth) {
		$self->_error('No statement handle passed.');
		return;	
	}
	my $retval;
	if (@_) {
		$retval = $sth->execute(@_);
	} else {
		$retval = $sth->execute();
	}
	unless ($retval) {
		$self->_error('sth->execute failed: '.$sth->errstr());
		$self->_error('sth->execute parameters: ['.join('], [', @_).']') if (@_);
		return;
	}
	return $retval;
}

# Record an error
sub _error {
	my $self = shift;
	my (@err) = @_;
	# check for error content
	unless ($self->{'error'}) {
		$self->{'error'} = [];
	}
	push @{$self->{'error'}}, @err;
}

1;
__END__

=pod
=head1 AUTHORS and MAINTAINERS
  Created by Clinton Graham F<E<lt>ctgraham@pitt.edu<gt>> 412-383-1057
=cut

