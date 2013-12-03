#!/usr/bin/perl

# Copyright (C) 2013 stepping stone GmbH
#                    Switzerland
#                    http://www.stepping-stone.ch
#                    support@stepping-stone.ch
#
# Authors:
#  Pat Kläy <pat.klaey@stepping-stone.ch>
#  
# Licensed under the EUPL, Version 1.1.
#
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
#
# http://www.osor.eu/eupl
#
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
#

package Provisioning;

################################################################################
##  Start pod2text documentation
################################################################################
=pod

=head1 Name

BackupKVMWrapper.pl

=head1 Usage
 
BackupKVMWrapper.pl -c|--config option_argument (( -i|--include) | (-e|--exclude)) option_argument [-d|--debug] [-r|--dry-run] [-h|--help]

=head1 Description

This wrapper will call the three sub processes (snapshot, merge and retain) for
a all machines in a given list. After all machines from the list have been 
snapshotted, the script will call merge and retain consecutively for each machine.

=over

=item snapshot

Saving the machines memory and CPU state to a file, moving the disk-image to the
retain location, creating a new (empty) disk image and restoring the machine
form the saved state file. 

=item merge

Merge the newly created disk image with the one of the retain location

=item retain

Copy the disk image and state file from retain location to the backup location

=back

=head1 Options

=over

=item -c|--config /path/to/your/configuration

This option is mandatory and specifies the backend (service) configuration
file.

=item -i|--inclide <list>

This mandatory (if -e|--exculde is not provided) option provides a list of all machines that should be processed in the run.
You have two possibilites to provide this list: 

=over

=item file:///path/to/list/file

You can specify a file, which contains the machine-names on seperate lines. The
'#' character indicates a comment, lines starting with '#' will be ignored.

=item "comma,separated,list"

You can directly pass the name of all machines in a comma separated list.

=back

=item -e|--exclude <list>

This mandatory (if -i|--include is not provided) option provides a list of all machines that should NOT be processed in the run. All other machines on your system will be backed up.
You have two possibilites to provide this list: 

=over

=item file:///path/to/list/file

You can specify a file, which contains the machine-names on seperate lines. The
'#' character indicates a comment, lines starting with '#' will be ignored.

=item "comma,separated,list"

You can directly pass the name of all machines in a comma separated list.

=back

=item -d|--debug

With this option, the script will print all log messages to STDOUT. 

=item -r|--dry-run

With this option, the script performs a dry-run. This means, the script only 
prints out what it would do. The system itself is not modified at all.

=item -h|--help

Display this help.

=back 

=head1 Examples

=over 

=item BackupKVMWrapper.pl -c /home/config/wrapper.conf- i"machine-01,machine-02"

This run takes the configuration from the file "/home/config/wrapper.conf" and 
will backup machine-01 and machine-02

=item BackupKVMWrapper.pl -c /home/config/wrapper.con f-i file:///home/config/machines.txt -d -r

This run takes the configuration from the file "/home/config/wrapper.conf",
prints all the messages to STDOUT and would pretend to backup all machines in 
"/home/config/machines.txt". But since the -r option is specified, no changes 
are made on the system.

=item BackupKVMWrapper.pl -c /home/config/wrapper.conf -e "machine-02"

This run takes the configuration from the file "/home/config/wrapper.conf" and 
will backup all machines on your system except for machine-02.

=item BackupKVMWrapper.pl -c /home/config/wrapper.conf -e ""

This run takes the configuration from the file "/home/config/wrapper.conf" and 
will backup ALL machines on your system.

=back

=cut

use Getopt::Long;
Getopt::Long::Configure("no_auto_abbrev");
use Config::IniFiles;
use Module::Load;
use Sys::Syslog;
use Cwd 'abs_path';
use File::Basename;
use LockFile::Simple qw(lock trylock unlock);
use POSIX;
use Sys::Virt;

# Flush the output
$|++;

sub get_lib{

  # to include the current directory as a search path in perl we have to 
  # do it in compile time, so the method gets the current directory and
  # returns it

  my $location=dirname(abs_path($0));
  return $location."/../../lib/";

}

# use the current directory as search path in perl
use lib get_lib();

# Open syslog
openlog("BackupKVMWrapper.pl","ndelay,pid", "local0");

# Write log message that the script was started
syslog("LOG_INFO","Starting Backup-KVM-Wrapper script");

# Get the options
my %opts;
GetOptions (
  \%opts,
  "help|h",             # Displays help text
  "debug|d",            # Enables debug mode
  "include|i:s",           # Comma seperated list or file which contains all machines
  "config|c:s",         # Specifys the configuration file
  "dryrun|r",           # Enables dry run mode
  "exclude|e:s"     # Comma seperated list or file which contains all machines which should not be backed up
);

# Get the scripts location
my $location = dirname(abs_path($0));

our $debug;
our $opt_d = $debug;

checkCommandLineArguments();

## Read the configuration file: 
our $cfg = new Config::IniFiles( -file => $opts{'config'} );

my $backend = $cfg->val("Database","BACKEND");

our $server_module = "Provisioning::Backend::$backend";
our $TransportAPI = $cfg->val("Service","TRANSPORTAPI");

our $global_cfg = $cfg;
our $syslog_name = $cfg->val("Service","SYSLOG");
our $opt_R = $opts{'dryrun'};

our $gateway_connection = 1;

# Load necessary modules
load "Provisioning::Log", ":all";
load "Provisioning::Backup::KVM", ':all';
load "Provisioning::Backend::$backend", ":all";
load "Provisioning::Backup::KVM::KVMBackup",':all';
load "Provisioning::TransportAPI::$TransportAPI",':all';

# Try to lock the script
my $lock = LockFile::Simple->make( -hold => 0 );
my $lock_file = "/var/run/Provisioning-Backup-KVM-Wrapper";
unless( $lock->trylock( $lock_file ) ){

  logger("warning","$service-Deamon already running (file $lock_file already locked), program exits now");
  exit;

}
else{
  logger("debug","file: $lock_file locked");
}

# Nice look and feel in debug mode
print "\n\n" if ( $debug );

# Connect to the backend
my $backend_connection = connectToBackendServer("connect", 1);

# Test if the connection could be established
unless ( defined($backend_connection) ) 
{
    # Log and exit
    logger("error","Cannot connect to backend, stopping here");
    exit 1;
}

# Generate the array machines list according to the list parameter
my @machines_list;
@machines_list = generateMachineList( $opts{'include'}, 1 ) if ( defined($opts{'include'}) );
@machines_list = generateMachineList( $opts{'exclude'}, 0) if ( defined($opts{'exclude'}) );

# Log which machines are going to be backed up
logger("debug","Backing up the following machines: @machines_list");

# Write the backup date to the server
my $backup_date = strftime("%Y%m%d%H%M%S",localtime());
modifyAttribute( "General",
                 "Backup_date",
                 $backup_date,
                 $backend_connection);

# Backup the machines
backupMachines( @machines_list );
   
logger("info","Backup-KVM-Wrapper script finished");

# Unlock the service
$lock->unlock($lock_file);

closelog();

################################################################################
# checkCommandLineArguments
################################################################################
# Description:
#  Check the command line arguments
################################################################################
sub checkCommandLineArguments 
{
    # Check if the user needs some help
    if ( $opts{'help'} )
    {
        syslog("LOG_INFO","Printing help...");
        exec("pod2text $location/".basename(abs_path($0)));
    }

    # Test if the user wants debug mode
    if ( $opts{'debug'} )
    {
        $debug = 1;
    }

    # Check if we have all necessary parameters i.e. config and list
    unless( $opts{'config'} )
    {
        # Log and exit
        syslog("LOG_ERR","No configuration file specified! You need to pass "
              ."a configuration file with the --config/-c option");
        exit 1;
    }

    if ( ! defined($opts{'include'}) && !defined($opts{'exclude'}) )
    {
        # Log and exit
        syslog("LOG_ERR","No list specified! You need to pass a list (either "
              ."comma seperated or with file:///path/to/file) with the --include/"
              ."-i or the --exclude/-e option");
        exit 1;
    }
    
    if ( defined($opts{'exclude'}) && defined($opts{'include'}) )
    {
            syslog("LOG_ERR","Cannot use --exclude and --include option together"
                          .". Use -h for more information");
            exit 1;
    }

} # end sub checkCommandLineArguments


################################################################################
# generateMachineList
################################################################################
# Description:
#  
################################################################################
sub generateMachineList
{
    my $list = shift;
    my $is_include_list = shift;

    # The list we will return
    my @machines = ();

    # Check if the list is already a list (comma seperated) or a file
    if ( $list =~ m/^file\:\/\// )
    {
        # It is a file, open it and parse it: 
        # Remove the file:// in front
        $list =~ s/file\:\/\///;

        # Check if the file is readable
        unless ( -r $list )
        {
            logger("error","Cannot read file $list, please make sure it exists "
                  ."and has correct permission");
            exit 1;
        } 

        # If the file is readable open and parse it
        open(FH,"$list");
        
        # Add all the lines / machine-names to the array
        while(<FH>)
        {
            chomp($_);
            push( @machines, $_ ) unless ($_ =~ m/^#/);
        }
        close FH;

    } else
    {
        # If the list is a comma seperated list, split it by comma
        @machines = split(",",$list);
    }

    # If it is an include list, we can return the list / array of machine names
    return @machines if ( $is_include_list );
    
    # If it is an exclude list, we need to get all machines on the current host
    my @all_machines; 
    my @not_running_machines;
    
    # Connect to libvirt
    my $vmm = Sys::Virt->new( addr => "qemu:///system" );

     # Get all machines running on the system
    eval
    {
        @all_machines = $vmm->list_domains();
    };

    # If there was an error, log it
    if ( $@ )
    {
        my $error_message = $@->message;
        logger("error","Cannot get machines, libvirt says: $error_message. Stopping here");
        exit 1;
    }

    # Get all defined but not running machines on the system
    eval
    {
        @not_running_machines = $vmm->list_defined_domains();
    };

    # If there was an error, log it
    if ( $@ )
    {
        my $error_message = $@->message;
        logger("error","Cannot get defined machines :libvirt says: $error_message. Stopping here");
        exit 1;
    }
    
    # Add the not running machines to the machines (we want all machines)
    push(@all_machines,@not_running_machines);

    # Test if there is at least one machine, if yes, we need the names of these machines
    if ( @all_machines == 0 )
    {
        logger("warning","No machines found on current system");
        return undef;
    } else
    {
        my $i = 0;
        while ( $all_machines[$i] )
        {
            # Get the machines name
            my $machine_name;
            eval
            {
                $machine_name = $all_machines[$i]->get_name();
            };
            
             # If there was an error, log it
            if ( $@ )
            {
                my $error_message = $@->message;
                logger("error","Cannot get machine name: libvirt says: $error_message. Stopping here");
                $i++;
                next;
            } else
            {
                $all_machines[$i] = $machine_name;
                $i++;
            }
        }
    }

    # Now remove the machines which should not be backed up
    my @backup_machines;
    foreach my $machine ( @all_machines )
    {
        if ( ! grep ( /^$machine$/, @machines) )
        {
            # Check if the machine name could be found, i.e. if the machine is a string now
            if ( $machine =~ m/[\w\s-]*/ )
            {
                push(@backup_machines,$machine);
            }
        }
    }
    
    # Finally return the backup machines
    return @backup_machines;

} # end sub generateMachineList

################################################################################
# backupMachines
################################################################################
# Description:
#  
################################################################################
sub backupMachines
{
    my @machines = @_;

    # Check hash if machine could successfully be snapshotted
    my %snapshot_success = ();

    # Go through all machines in the list passed and execute the snapshot method
    foreach my $machine ( @machines ) 
    {
        # Log which machine we are processing
        logger("debug","Snapshotting machine $machine");

        # At start error is 0 for every machine 
        my $error = 0;

        $error = processEntry($machine,"snapshot");

        # Test if there was an error
        if ( $error )
        {
            logger("error","Snapshot process returned error code: $error"
                  ." Will not call merge and retain processes for machine "
                  ."$machine.") if $error != -1;
            $snapshot_success{$machine} = "no";
            next;
        }

        $snapshot_success{$machine} = "yes";
        logger("debug","Machine $machine successfully snapshotted");

    } # End of for each

    # After all machines have been snapshotted, we can merge and retain them
    foreach my $machine ( @machines )
    {
        
        # Check if the snapshot for this machine was successfull
        next if $snapshot_success{$machine} eq "no";

        logger("debug","Processing (merge and retain) machine $machine");

        # Do the merge and retain
        $error = processEntry($machine,"merge");

        # Test if there was an error
        if ( $error )
        {
            logger("error","Merge process returned error code: $error"
                  ." Will not call retain processes for machine "
                  ."$machine.");
            next;
        }

        $error = processEntry($machine,"retain");

        # Test if there was an error
        if ( $error )
        {
            logger("error","Retain process returned error code: $error"
                  ." No files have beed transfered to the backup location");
            next;
        }

        checkIterations($machine);

        logger("debug","Machine $machine processed");

    }

}

################################################################################
# checkIterations
################################################################################
# Description:
#  
################################################################################
sub checkIterations
{
    my $machine = shift;

    my @iterations;

    # Get the backup directory for the machine
    my $backup_directory = getValue($machine, "sstBackupRootDirectory");

    # Remove the file:// in front
    $backup_directory =~ s/file\:\/\///;

    # Add the intermediate path 
    $backup_directory .= "/".returnIntermediatePath()."/..";

    # Go through the hole directory and put all iterations in the iterations 
    # array
    while(<$backup_directory/*>)
    {
        push( @iterations, basename( $_ ) );
    }

    # As long as we have more iterations as we should have, delete the oldest:
    while ( @iterations > getValue( $machine, "SSTBACKUPNUMBEROFITERATIONS") )
    {
        # Set the oldest date to year 9000 to make sure that whatever date will
        # follow is older
        my $oldest = "90000101000000";
        my $oldest_index = 0;
        my $index;

        # Go through all iterations an check if the current iteration is older 
        # than the current oldest one. If yes, set the current iteration as the
        # oldest iteration.
        foreach my $iteration (@iterations)
        {
            if ( $iteration < $oldest )
            {
                $oldest = $iteration;
                $oldest_index = $index;
            } else
            {
                $index++;
            }
            
        }

        # Delete the oldest iteration from the array: 
        splice @iterations, $oldest_index, 1;

        # And delete it also from the filesystem: 
        my @args = ( "rm", "-rf", $backup_directory."/".$oldest);
        if ( executeCommand( undef, @args ) )
        {
            logger("error","Could not delete oldest iteration: "
                  .$backup_directory."/".$oldest.", try to delete it manually "
                  ."by executing the following command: @args");
            return 1;
        } else
        {
            logger("debug","Oldest iteration successfully deleted\n");
        }

    }


}