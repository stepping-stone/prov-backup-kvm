package Provisioning::Backup::KVM::KVMBackup;

# Copyright (C) 2012 FOSS-Group
#                    Germany
#                    http://www.foss-group.de
#                    support@foss-group.de
#
# Authors:
#  Pat Kläy <pat.klaey@stepping-stone.ch>
#  
# Licensed under the EUPL, Version 1.1 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
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
#

use warnings;
use strict;

use Config::IniFiles;
use Switch;
use Module::Load;
use POSIX;
use Sys::Virt;
use XML::Simple;
use Filesys::Df;
use Sys::Hostname;
use File::Basename;

use Provisioning::Log;
use Provisioning::Util;
use Provisioning::Backup::KVM::Constants;

require Exporter;

=pod

=head1 Name

KVMBackup.pm

=head1 Description

=head1 Methods

=cut


our @ISA = qw(Exporter);

our %EXPORT_TAGS = ( 'all' => [ qw(backup) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(backup);

our $VERSION = '0.01';

use constant 
{

SUCCESS_CODE => Provisioning::Backup::KVM::Constants::SUCCESS_CODE,
ERROR_CODE => Provisioning::Backup::KVM::Constants::ERROR_CODE,

};


# Get some vars from the provisioning script
my $dry_run = $Provisioning::opt_R;
my $TransportAPI = "Provisioning::TransportAPI::$Provisioning::TransportAPI";
my $gateway_connection = "$Provisioning::gateway_connection";

load "$TransportAPI", ':all';
load "$Provisioning::server_module", ':all';

# Define the VMmanager:
my $vmm = Sys::Virt->new( addr => "qemu:///system" );

# Set a variable to save the intermediate path to the disk image
my $intermediate_path;

################################################################################
# backup
################################################################################
# Description:
#  
################################################################################

sub backup
{

    my ($state, $entry , $backend_connection , $cfg ) = @_;

    # Initialize the var to return any error, initially it is 0 (no error)
    my $error = 0;

    # Get the machine according the the backend entry:
    my $backend = $cfg->val("Database","BACKEND");

    my $machine = getMachineByBackendEntry( $entry, $backend );

    if ( !$machine )
    {
        # Log and exit
        logger("error","Did not find machine according to the backend entry"),
        return Provisioning::Backup::KVM::Constants::CANNOT_FIND_MACHINE;
    }

    # Get the machines name:
    my $machine_name = getMachineName($machine);

    # Test if we could get the machines name
    unless ( defined( $machine_name ) )
    {
        # Return error code cannot save machines state ( we cannot save the
        # machine if we don't know the name)
        return Provisioning::Backup::KVM::Constants::UNDEFINED_ERROR;
    }

    # Get and set the intermediate path for the given machine
    $intermediate_path = setIntermediatePath( $machine );

    # Get the parents enry because there is the configuration
    my $config_entry = getConfigEntry($entry, $cfg);

    # Test if a configuration entry was found or whether it is the error
    # Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY
    if ( $config_entry == Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY ) 
    {
        return Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY;
    }

    # Test what kind of state we have and set up the appropriate action
    switch ( $state )
    {
        case "snapshotting" {   # Measure the start time:
                                my $start_time = time;

                                # Create a snapshot of the machine
                                # Save the machines state
                                my $state_file;
                                ( $state_file, $error ) = 
                                saveMachineState( $machine,
                                                  $machine_name,
                                                  $config_entry,
                                                  $cfg );

                                if ( $error )
                                {
                                    # Log the error
                                    logger("error","Saving machine state for "
                                          ."$machine_name failed with "
                                          ."error code: $error");

                                    # Test if machine is running, if not start
                                    # it!
                                    my $running;
                                    eval
                                    {
                                        $running = $machine->is_active();
                                    };

                                    # Test if there was an error, if yes log it
                                    my $libvirt_error = $@;
                                    if ( $libvirt_error )
                                    {
                                        logger("error","Could not check machine"
                                              ." state (running or not) for "
                                              ."machine $machine_name, libvirt "
                                              ."says: '".$libvirt_error->message
                                              ."'. Please "
                                              ."execute the following command "
                                              ."on ".hostname." to check "
                                              ."whether or not the machine is "
                                              ."running: virsh list --all"
                                              );

                                        # Set running to true to avoid that
                                        # libvirt tries to start an already 
                                        # started domain
                                        $running = 1;
                                    }
                                    
                                    # If the machine is not running, start it
                                    unless ( $running )
                                    {
                                        # Start the machine
                                        eval
                                        {
                                            $machine->create();
                                        };

                                        # Test if there was an error
                                        my $libvirt_error = $@;
                                        if ( $libvirt_error )
                                        {
                                            logger("error","Could not start the"
                                                  ." machine $machine_name. "
                                                  ."Libvit says: ".
                                                  $libvirt_error->message
                                                  );
                                        }
                                        
                                    }

                                    # Return that the machines state could not
                                    # be saved
                                    return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_MACHINE_STATE;
                                }

                                # Success, log it!
                                logger("debug","Machines ($machine_name) state "
                                       ."successfully saved to $state_file");

                                # Rename the original disk image and create a 
                                # new empty one which will  be used to write 
                                # further changes to.
                                my $disk_image;
                                ($disk_image, $error) = changeDiskImages( 
                                                            $machine , 
                                                            $machine_name , 
                                                            $config_entry ,
                                                            $cfg );

                                # Check if there was an error
                                if ( $error )
                                {
                                    # Log the error
                                    logger("error","Changing disk images for "
                                          ."$machine_name failed with error "
                                          ."code: $error");

                                    # Try to restore the VM if this is not
                                    # successful log it but return the previous
                                    # error!
                                    logger("info","Trying to restore the VM "
                                          .$machine_name);

                                    if ( restoreVM($machine_name,$state_file) )
                                    {
                                        logger("error","Could not restore VM "
                                              ."$machine_name!!!"
                                              );
                                    }


                                    # Return the error
                                    return $error;
                                }

                                # Success log it
                                logger("debug","Successfully changed the disk "
                                      ."images for machine $machine_name");

                                # Now we can restore the VM from the saved state
                                if ($error=restoreVM($machine_name,$state_file))
                                {
                                    # This is pretty bad, if we cannot restore 
                                    # the VM log it and set up the appropriate 
                                    # action

                                    #TODO maybe change this to disaster or fatal
                                    logger("error","Restoring machine "
                                          ."$machine_name failed with error "
                                          ."code: $error");

                                    # TODO we need to act here, what should be done?

                                    return Provisioning::Backup::KVM::Constants::CANNOT_RESTORE_MACHINE;
                                }

                                # Success, log it
                                logger("debug","Machine $machine_name "
                                      ."successfully restored from $state_file");

                                # Copy the file from the ram disk to the retain
                                # location if not already there
                                my $retain_directory = getValue($config_entry,
                                                    "sstBackupRetainDirectory");

                                # Remove the file:// in front of the retain
                                # directory
                                $retain_directory =~ s/file:\/\///;

                                unless ( $state_file =~ m/$retain_directory/)
                                {
                                    # Log what we are doing
                                    logger("debug","Coping state file to retain"
                                          ." directory");

                                    # Get the state file name
                                    my $state_file_name = basename($state_file);

                                    # Copy the state file to the retain location
                                    if ( $error = exportFileToLocation($state_file, "file://".$retain_directory."/".$intermediate_path, "" ,$cfg) )
                                    {
                                        # Log what went wrong and return 
                                        # appropriate error code
                                        logger("error","Exporting save file to "
                                              ."retain direcotry failed with "
                                              ."error code $error");

                                        return Provisioning::Backup::KVM::Constants::CANNOT_COPY_STATE_FILE_TO_RETAIN
                                        
                                    } else
                                    {
                                        # Log that everything went fine
                                        logger("debug","State file successfully"
                                              ." copied to retain directory");

                                       # Remove the file from RAM-Disk
                                        deleteFile("file://".$state_file);
                                    }
                                } else
                                {
                                    # Log that the file is already where it
                                    # should be!
                                    logger("debug","State file ($state_file) "
                                          ."already at retain location, nothing"
                                          ." to do.");
                                }

                                # Save the XML of the machine
                                my $xml = $retain_directory."/".
                                          $intermediate_path."/".
                                          $machine_name.".xml";

                                # Save the XML and get back the status
                                $error = saveXMLDescription( $machine, $xml, $cfg );

                                # Check if there was an error
                                if ( $error )
                                {
                                    # Log it and 
                                    # TODO how do we proceed here? warning and 
                                    # continue or stop
                                    logger("warning","Could not save XML for"
                                          ." machine $machine_name");
                                }

                                # Log success
                                logger("debug","XML description for machine "
                                      ."$machine_name successfully saved");

                                # Save the LDIF
                                my $backend_entry = $retain_directory."/".
                                                    $intermediate_path."/".
                                                    $machine_name.".%backend%";

                                $error = saveBackendEntry( $entry, $backend_entry, $backend, $cfg );

                                # Check if there was an error
                                if ( $error )
                                {
                                    # Log it and 
                                    # TODO how do we proceed here? warning and 
                                    # continue or stop
                                    logger("warning","Could not save backend "
                                          ."entry for machine $machine_name");
                                }

                                # Log success
                                logger("debug","Bachend entry for machine "
                                      ."$machine_name successfully saved");

                                # Write that the snapshot process is finished
                                modifyAttribute (  $entry,
                                                   "sstProvisioningMode",
                                                   "snapshotted",
                                                   $backend_connection,
                                                 );

                                # Measure end time
                                my $end_time = time;

                                # Calculate the duration
                                my $duration = $end_time - $start_time;

                                # Write the duration to the LDAP
                                writeDurationToBackend($entry,"snapshot",$duration,$backend_connection);

                                return $error;

                            } # End case snapshotting

        case "merging"      { # Merge the disk images

                                # Measure the start time:
                                my $start_time = time;

                                # Get the disk image
                                my $disk_image = getDiskImageByMachine($machine);

                                # Get the bandwidth in MB
                                my $bandwidth = getValue($config_entry,"sstVirtualizationBandwidthMerge");

                                # If the bandwidth is 0, it means unlimited, 
                                # since libvirt does not yet support it, set it
                                # big enough
                                $bandwidth = 2000 if ( $bandwidth == 0 );

                                if ( $error = mergeDiskImages( $machine, $disk_image, $bandwidth, $machine_name ) )
                                {
                                    # Log and return an error
                                    logger("error","Merging disk images for "
                                          ."machine $machine_name failed with "
                                           ."error code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_MERGE_DISK_IMAGES;
                                }

                                # Write that the merge process is finished
                                modifyAttribute (  $entry,
                                                   "sstProvisioningMode",
                                                   "merged",
                                                   $backend_connection,
                                                 );

                                # Measure end time
                                my $end_time = time;

                                # Calculate the duration
                                my $duration = $end_time - $start_time;

                                # Write the duration to the LDAP
                                writeDurationToBackend($entry,"merge",$duration,$backend_connection);

                                return $error;
                            } # End case merging

        case "retaining"    { # Retain the old files

                                # Measure the start time:
                                my $start_time = time;

                                # Get the retain and backup location: 
                                my $retain_location = getValue($config_entry,
                                                    "sstBackupRetainDirectory");
                                my $backup_directory = getValue($config_entry,
                                                      "sstBackupRootDirectory");

                                # Check if the retain and backup root directory 
                                # exist
                                # Remove file:// in front to test
                                my $retain_test_location = $retain_location;
                                $retain_test_location =~ s/file\:\/\///;

                                unless ( -d $retain_test_location )
                                {
                                    # Log and return 
                                    logger("error","Retain root directory ("
                                          ."$retain_test_location) does not "
                                          ."exist. Stopping here"
                                          );
                                    return Provisioning::Backup::KVM::Constants::RETAIN_ROOT_DIRECTORY_DOES_NOT_EXIST;
                                }

                                # Remove file:// in front to test
                                my $backup_test_directory = $backup_directory;
                                $backup_test_directory =~ s/file\:\/\///;

                                unless ( -d $backup_test_directory )
                                {
                                    # Log and return 
                                    logger("error","Backup root directory ("
                                          ."$backup_test_directory) does not "
                                          ."exist. Stopping here"
                                          );
                                    return Provisioning::Backup::KVM::Constants::BACKUP_ROOT_DIRECTORY_DOES_NOT_EXIST;
                                }

                                # Get disk image and state file
                                # Get the disk image
                                my $disk_image= getDiskImageByMachine($machine);
                                my $disk_image_name = basename( $disk_image );
                                $disk_image = $retain_location."/"
                                             .$intermediate_path."/"
                                             .$disk_image_name.".backup";
                                             
                                # Get the state file
                                my $state_file = $retain_location."/".
                                                 $intermediate_path."/".
                                                 $machine_name.".state";

                                # Get the xml file
                                my $xml_file = $retain_location."/".
                                               $intermediate_path."/".
                                               $machine_name.".xml";

                                # Get the backend file
                                my $backend_file = $retain_location."/".
                                                   $intermediate_path."/".
                                                   $machine_name;

                                # Add the correct backend extension: 
                                switch ( $backend )
                                {
                                    case "LDAP" { $backend_file .= ".ldif"; }
                                    else { $backend_file .= ".export"; }
                                }

                                # get the ou of the current entry to add as 
                                # suffix to the image and state file
                                my $suffix = getValue($entry,"ou");

                                # Export the state file and the old disk image 
                                # to the backup location
                                if ( $error = exportFileToLocation($state_file,$backup_directory."/".$intermediate_path,".$suffix",$cfg))
                                {
                                    # If an error occured log it and return 
                                    logger("error","State file ('$state_file') "
                                          ."transfer to '$backup_directory"
                                          ."/$intermediate_path' "
                                          ."failed with return code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_COPY_STATE_TO_BACKUP_LOCATION;
                                }

                                # Success, log it!
                                logger("debug","Successfully exported state "
                                      ."file for machine $machine_name"
                                      ." to '$backup_directory'");

                                # export the disk image
                                if ( $error = exportFileToLocation($disk_image,$backup_directory."/".$intermediate_path,".$suffix",$cfg))
                                {
                                    # If an error occured log it and return 
                                    logger("error","Disk image ('$disk_image') "
                                          ."transfer to '$backup_directory"
                                          ."/$intermediate_path' "
                                          ."failed with return code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_COPY_IMAGE_TO_BACKUP_LOCATION;
                                }

                                # Success, log it!
                                logger("debug","Successfully exported disk "
                                      ."image for machine $machine_name"
                                      ." to '$backup_directory'");

                                # export the xml file
                                if ( $error = exportFileToLocation($xml_file,$backup_directory."/".$intermediate_path,".$suffix",$cfg))
                                {
                                    # If an error occured log it and return 
                                    logger("error","Xml file ('$xml_file') "
                                          ."transfer to '$backup_directory"
                                          ."/$intermediate_path' "
                                          ."failed with return code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_COPY_XML_TO_BACKUP_LOCATION;
                                }

                                # Success, log it!
                                logger("debug","Successfully exported xml "
                                      ."file for machine $machine_name"
                                      ." to '$backup_directory'");

                                # export the disk image
                                if ( $error = exportFileToLocation($backend_file,$backup_directory."/".$intermediate_path,".$suffix",$cfg))
                                {
                                    # If an error occured log it and return 
                                    logger("error","Backend file ('$backend_file') "
                                          ."transfer to '$backup_directory"
                                          ."/$intermediate_path' "
                                          ."failed with return code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_COPY_BACKEND_FILE_TO_BACKUP_LOCATION;
                                }

                                # Success, log it!
                                logger("debug","Successfully exported backend "
                                      ."file for machine $machine_name"
                                      ." to '$backup_directory'");

                                # And finally clean up the no lgner needed files
                                if ( $error = deleteFile( $state_file ) )
                                {
                                    # If an error occured log it and return 
                                    logger("error","Deleting file $state_file "
                                          ."failed with return code: $error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_REMOVE_FILE;
                                }

                                # And finally clean up the no lgner needed files
                                if ( $error = deleteFile( $disk_image ) )
                                {
                                    # If an error occured log it and return 
                                    logger("error","Deleting file $disk_image."
                                          ."backup failed with return code: "
                                          ."$error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_REMOVE_FILE;
                                }

                                # And finally clean up the no lgner needed files
                                if ( $error = deleteFile( $xml_file ) )
                                {
                                    # If an error occured log it and return 
                                    logger("error","Deleting file $xml_file "
                                          ."failed with return code: "
                                          ."$error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_REMOVE_FILE;
                                }

                                # And finally clean up the no lgner needed files
                                if ( $error = deleteFile( $backend_file ) )
                                {
                                    # If an error occured log it and return 
                                    logger("error","Deleting file $backend_file"
                                          ." failed with return code: "
                                          ."$error");
                                    return Provisioning::Backup::KVM::Constants::CANNOT_REMOVE_FILE;
                                }

                                # Write that the merge process is finished
                                modifyAttribute (  $entry,
                                                   "sstProvisioningMode",
                                                   "retained",
                                                   $backend_connection,
                                                 );

                                # Measure end time
                                my $end_time = time;

                                # Calculate the duration
                                my $duration = $end_time - $start_time;

                                # Write the duration to the LDAP
                                writeDurationToBackend($entry,"retain",$duration,$backend_connection);


                                # return the status
                                return $error;


                            } # End case retaining
        else                { # If nothing of the above was true we have a
                              # problem, log it and return appropriate error 
                              # code
                              logger("error","State $state is not known in "
                                    ."KVM-Backup.pm. Stopping here.");
                              return Provisioning::Backup::KVM::Constants::WRONG_STATE_INFORMATION;
                            }


    } # End switch $state
}


################################################################################
# backupSingleMachine
################################################################################
# Description:
#  
################################################################################

sub saveMachineState
{

    my ( $machine, $machine_name, $entry, $cfg ) = @_;

    # Initialize the var to return any error, initially it is 0 (no error) 
    my $error = 0;

    # State file, this is important because it will be returned on success
    my $state_file;

    # Intialize the variable which holds the path the the state file
    my $save_state_location;

    # Get the retain location in case no ram disk is configured
    my $retain_location = getValue($entry,"sstBackupRetainDirectory");

    # Remove the file:// in front
    $retain_location =~ s/file:\/\///;

    # Check if the retain directory as is exists
    unless ( -d $retain_location )
    {
        # If ot does not, write an log message and return
        logger("error","Retain directory ($retain_location) does not exist"
              ." please create it by executing the following command (script "
              ."stopps here!): mkdir -p $retain_location" );
        return "",Provisioning::Backup::KVM::Constants::RETAIN_ROOT_DIRECTORY_DOES_NOT_EXIST;
    }

    # Add the intermediate path to the retain location
    $retain_location .= "/".$intermediate_path;

    # Test if the location where we should put the disk image exists
    unless ( -d $retain_location )
    {
        # Create it
        if ( createDirectory( $retain_location, $cfg ) != SUCCESS_CODE )
        {
            # There was an error in creating the directory log it
            logger("error","Failed to create directory $retain_location,"
                  ." cannot move disk image to retain location, stopping here"
                  );
            return "",Provisioning::Backup::KVM::Constants::CANNOT_CREATE_DIRECTORY;
        }
        
    }

    # What are we doing?
    logger("debug","Saving state for machine $machine_name");

    # Check if a ram disk is configured
    my $ram_disk = getValue($entry, "sstBackupRamDiskLocation");

    if ( $ram_disk )
    {
        # We are using RAM-Disk
        logger("debug","RAM-Disk configured, using it to save state");

        # Get the RAM-Disk location
        my $ram_disk_location = $ram_disk;
        
        # Remove the file:// in front of the ram disk location
        $ram_disk_location =~ s/file:\/\///;

        # Test if we can write to the specified location
        if ( -w $ram_disk_location ) 
        {
            # Test if the specified RAM-Disk is large enough
            if ( checkRAMDiskSize($machine,$ram_disk_location) == SUCCESS_CODE )
            {

                # Everything is ok, we can use the RAM-Disk
                $save_state_location = $ram_disk_location;

            } else
            {

                # Log that the RAM-Disk is not large enogh
                logger("warning","Configured RAM-Disk (".$ram_disk_location.") "
                       ."is not large enough to save machine, taking retain "
                       ."backup location to save state file" );

                # If the RAM-Disk is not large enogh, save the state to the 
                # local backup location
                $save_state_location = $retain_location;

            } # End else from if ( checkRAMDiskSize( $entry, $ram_disk_location ) )

        } else
        {
            # If we cannot write to the RAM Disk, log it and use local backup
            # location:
            logger("warning","Configured RAM-Disk (".$ram_disk_location.") is "
                  ."not writable, please make sure it exists and has correct "
                  ."permission, taking retain backup location to save state file"
                  );
            # If the RAM-Disk is not writable take retain location
            $save_state_location = $retain_location;

        } # End else from if ( -w $ram_disk_location )

    } else
    {
        # Log that no RAM-Disk is configured
        logger("debug","No RAM-Disk configured, taking local backup location "
               ."to save state file" );

        # If no RAM-Disk is configured, use the local backup location
        $save_state_location = $retain_location;

    } # End else from if ( $ram_disk )

    # Log the location we are going to save the machines state
    logger("debug","Saving state of machine $machine_name to "
           ."$save_state_location");

    # Specify a helpy variable
    $state_file = $save_state_location."/$machine_name.state";

    # Save the VMs state, either in dry run or really
    if ( $dry_run )
    {
        # Print what we would do to save the VMs state
        print "DRY-RUN:  ";
        print "virsh save $machine_name $state_file";
        print "\n\n";
        
        # Show dots for three seconds
        showWait(3);
        
    } else
    {
        # Save the machines state, put it into an eval block to avoid that the 
        # whole programm crashes if something fails
        eval
        {
            $machine->save($state_file);
        };

        my $libvirt_err = $@;
               
        # Test if there was an error
        if ( $libvirt_err )
        {
            my $error_message = $libvirt_err->message;
            $error = $libvirt_err->code;
            logger("error","Error from libvirt (".$error
                  ."): libvirt says: $error_message.");
            return "",$error;
        }
    }

    return ( $state_file , $error );

}

################################################################################
# checkRAMDiskSize
################################################################################
# Description:
#  
################################################################################

sub changeDiskImages
{

    my ( $machine , $machine_name , $config_entry ,$cfg ) = @_;

    # Initialize the var to return any error, initially it is 0 (no error) 
    my $error = 0;

    # Log what we are currently doing
    logger("debug","Renaming original disk image for machine $machine_name");

    # First of all we need the name and location of the disk image
    my $disk_image = getDiskImageByMachine( $machine );

    # If the disk image was not found, return with appropriat error
    unless ( $disk_image )
    {
        # Write log and return 
        logger("error","Could not get disk image for machine $machine_name");
        return "",Provisioning::Backup::KVM::Constants::CANNOT_RENAME_DISK_IMAGE;
    }

    # If we got the disk image we can rename / move it using the TransportAPI
    # So first generate the commands:
    # Get the disk image name
    my $disk_image_name = basename( $disk_image );
    
    # Get the retain locatio:
    my $retain_directory = getValue($config_entry,"sstBackupRetainDirectory");

    # Remove the file:// in front
    $retain_directory =~ s/^file:\/\///;

    # Add the intermediate path to the retain location
    $retain_directory .= "/".$intermediate_path;

    # Test if the location where we should put the disk image exists
    unless ( -d $retain_directory )
    {
        # Create it
        if ( createDirectory( $retain_directory, $cfg ) != SUCCESS_CODE )
        {
            # There was an error in creating the directory log it
            logger("error","Failed to create directory $retain_directory,"
                  ." cannot move disk image to retain location, stopping here"
                  );
            return "",Provisioning::Backup::KVM::Constants::CANNOT_CREATE_DIRECTORY;
        }
        
    }
                                    
    my @args = ('mv',$disk_image,$retain_directory."/".$disk_image_name.'.backup');

    # Execute the commands
    my ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not move the  disk image for machine "
               ."$machine_name: error: $command_err" );
        return "",Provisioning::Backup::KVM::Constants::CANNOT_RENAME_DISK_IMAGE;
    }

    # When the disk image could be renamed log it and continue
    logger("debug","Disk image renamed for machine $machine_name");
    logger("debug","Creating new disk image for machine $machine_name");

    # Create a new disk image with the same name as the old (original one) and
    # set correct permission
    if ( $error = createEmptyDiskImage($disk_image,$config_entry,$retain_directory."/".$disk_image_name.'.backup'))
    {
        # Log it and return
        logger("error","Could not create empty disk for machine $machine_name"
               .": error: $error");
        return "",$error;
    }
    # If the new image is created and has correct permission this method is done
    return ($disk_image, $error);
    
}


################################################################################
# checkRAMDiskSize
################################################################################
# Description:
#  
################################################################################

sub restoreVM
{
    my ( $machine_name, $state_file ) = @_;

    # Log what we are doing
    logger("debug","Restoring machine $machine_name from $state_file");

    # Initialize error to no-error
    my $error = 0;

    # Check whether the specified state file is readable (only if not in dry
    # run)
    if ( !(-r $state_file) && !$dry_run )
    {
        # Log it and return error
        logger("error","Cannot read state file '$state_file' for machine "
               .$machine_name );
        $error = 1;
        return $error;
    }

    # Otherwise restore the machine
    if ( $dry_run )
    {
        # Print what we would do to save the VMs state
        print "DRY-RUN:  ";
        print "virsh restore $state_file";
        print "\n\n";
        
        # Show the dots for 5 seconds
        showWait(5);    
    } else
    {
        # Really restore the machine using libvirt api, put it also in an eval 
        # block to avoid the programm to crash if anything goes wrong
        eval
        {
            $vmm->restore_domain($state_file);
        };

        my $libvirt_err = $@;
               
        # Test if there was an error
        if ( $libvirt_err )
        {
            my $error_message = $libvirt_err->message;
            $error = $libvirt_err->code;
            logger("error","Error from libvirt (".$error
                  ."): libvirt says: $error_message.");
            return $error;
        }

    }

    # Log success
    logger("debug", "Machine $machine_name successfully restored!");
    return $error;

}


################################################################################
# mergeDiskImages
################################################################################
# Description:
#  
################################################################################

sub mergeDiskImages
{

    my ( $machine, $disk_image, $bandwidth, $machine_name ) = @_;

    # Initialize error to no-error
    my $error = 0;

    # Log what we are doing
    logger("debug","Merging disk images for machine $machine_name which is "
           ."the following file: $disk_image");

    # If in dry run just print what we would do
    if ( $dry_run )
    {
        # Print what we would do to merge the images
        print "DRY-RUN:  ";
        print "virsh qemu-monitor-command --hmp $machine_name 'block_stream ";
        print "drive-virtio-disk0' --speed $bandwidth";
        print "\n\n";

        # Show dots for 30 seconds
        showWait(30);
    } else
    {
        # Really merge the disk images
        eval
        {
            $machine->block_pull($disk_image, $bandwidth);
        };

        my $libvirt_err = $@;
               
        # Test if there was an error
        if ( $libvirt_err )
        {
            my $error_message = $libvirt_err->message;
            $error = $libvirt_err->code;
            logger("error","Error from libvirt (".$error
                  ."): libvirt says: $error_message.");
            return $error;
        }

        # Test if the job is done:
        my $job_done = 0;
        while ( $job_done == 0)
        {
            # Get the block job information from the machine and the given image
            my $info = $machine->get_block_job_info($disk_image, my $flags=0);

            # Test if type == 0, if yes the job is done, if test == 1, the job 
            # is still running
            $job_done = 1 if ( $info->{type} == 0 );

	    # Wait for a second and retest
	    sleep(1);
        }
        
    }

    return $error;    
}


################################################################################
# checkRAMDiskSize
################################################################################
# Description:
#  
################################################################################

sub exportFileToLocation
{
    my ($file, $location, $suffix ,$cfg) = @_;

    # Remove the file:// in front of the file
    $file =~ s/^file:\/\///;

    # Calculate the export command depending on the $location prefix (currently 
    # only file:// => cp -p is supported)
    my $command;
    if ( $location =~ m/^file:\/\// )
    {
        # Remove the file:// in front of the actual path
        $location =~ s/^file:\/\///;
        $command = "cp -p";
    }
    # elsif (other case) {other solution}

    # Get the file name:
    my $file_name = basename($file);

    # Add the siffix at the end of the file name
    $file_name .= $suffix;

    # What are we doing? 
    logger("debug","Exporting $file to $location using $command");

    # Test if the destination directory exists, if not, create it
    unless ( -d $location )
    {
        # Create the directory
        if ( createDirectory( $location, $cfg ) != SUCCESS_CODE )
        {
            # Log it and return
            logger("error", "Failed to create directory $location,"
                  ." cannot move file there. Stopping here");
            return Provisioning::Backup::KVM::Constants::CANNOT_CREATE_DIRECTORY;
        }

        # Log succes
        logger("debug", "Destination directory $location "
              ."successfully created");
    }

    # Genereate the command
    my @args = ($command,$file,$location."/".$file_name);

    # Execute the command
    my ($output, $error) = executeCommand($gateway_connection, @args);

    # Test whether or not the command was successfull: 
    if ( $error )
    {
        # If there was an error log what happend and return 
        logger("error","Could export $file to $location. Error: $error" );
        return $error;
    }

    # Return
    return $error;
}

################################################################################
# deleteFile
################################################################################
# Description:
#  
################################################################################

sub deleteFile
{
    my $file = shift;

    # Remove file:// in front of the file
    $file =~ s/file:\/\///;

    # Log what we are doing
    logger("debug","Deleting file $file");

    # Generate the command
    my @args = ("rm",$file);

    # Execute the command
    # Execute the command
    my ($output, $error) = executeCommand($gateway_connection, @args);

    # Test whether or not the command was successfull: 
    if ( $error )
    {
        # If there was an error log what happend and return 
        logger("error","Could delete file $file. Error: $error" );
        return $error;
    }

    # Return
    return $error;
}

################################################################################
# checkRAMDiskSize
################################################################################
# Description:
#  
################################################################################

sub checkRAMDiskSize
{
    my ($machine, $dir ) = @_;

    # Get filesytem information for the specified directory (size in KB)
    my $file_system_info = df($dir);

    my $info;
    eval
    {
        $info = $machine->get_info();
    };

    my $libvirt_err = $@;
               
    # Test if there was an error
    if ( $libvirt_err )
    {
        my $error_message = $libvirt_err->message;
        my $error = $libvirt_err->code;
        logger("error","Error from libvirt (".$error
              ."): libvirt says: $error_message.");
        return $error;
    }

    # Get the current allocated memory of the domain in KB
    my $ram = $info->{memory};

    # Now add add 10%  for the cpu state
    $ram *= 1.05;

    # Check whether the available space on the ram disk is large enogh
    if ( $ram < $file_system_info->{bavail} )
    {
        # Ram disk is large enough
        return SUCCESS_CODE;
    } else
    {
        # Ram disk is too small
        return ERROR_CODE;
    }

    return SUCCESS_CODE;
}


################################################################################
# getMachineByBackendEntry
################################################################################
# Description:
#  
################################################################################

sub getMachineByBackendEntry
{

    my ( $entry, $backend ) = @_;

    # The machines name:
    my $name;

    # Test what kind of backend we have
    switch ( $backend )
    {
        case "LDAP" {
                        # First of all we need the dn because the machine name
                        # is part of the dn
                        my $dn = getValue($entry,"dn");
                        
                        # The attribute is sstVirtualMachine so search for it
                        $dn =~ m/,sstVirtualMachine=(.*),ou=virtual\s/;
                        $name = $1;
                    }
        else        {
                        # We don't know the backend, log it and return undef
                        logger("error","Backend type '$backend' unknown, cannot"
                              ." get machine");
                        return undef;
                    }
    }

    # Then get the machine by the name:
    my $machine;
    eval
    {
        $machine = $vmm->get_domain_by_name($name);
    };

    my $libvirt_err = $@;
               
    # Test if there was an error
    if ( $libvirt_err )
    {
        my $error_message = $libvirt_err->message;
        my $error = $libvirt_err->code;
        logger("error","Error from libvirt (".$error
              ."): libvirt says: $error_message.");
        return undef;
    }
   
    return $machine;
 }


################################################################################
# getMachineName
################################################################################
# Description:
#  
################################################################################

sub getMachineName
{

    my $machine = shift;

    my $name;
    eval
    {
        $name = $machine->get_name();
    };

    my $libvirt_err = $@;
               
    # Test if there was an error
    if ( $libvirt_err )
    {
        my $error_message = $libvirt_err->message;
        my $error = $libvirt_err->code;
        logger("error","Error from libvirt (".$error
              ."): libvirt says: $error_message.");
        return undef;
    }

    return $name;
}


################################################################################
# getMachineName
################################################################################
# Description:
#  
################################################################################

sub getDiskImageByMachine
{
    my $machine = shift;

    # Get the machines xml description 
    my $xml_string;
    eval
    {
        $xml_string = $machine->get_xml_description();
    };

    my $libvirt_err = $@;
               
    # Test if there was an error
    if ( $libvirt_err )
    {
        my $error_message = $libvirt_err->message;
        my $error = $libvirt_err->code;
        logger("error","Error from libvirt (".$error
              ."): libvirt says: $error_message.");
        return undef;
    }

    # Will be used to go through the xml
    my $i = 0;

    # Initialize the XML-object from the string
    my $xml = XMLin( $xml_string,
                     KeepRoot => 1,
                     ForceArray => 1
                   );

    # Go through all domainsnapshot -> domain -> device -> disks
    while ( $xml->{'domain'}->[0]->{'devices'}->[0]->{'disk'}->[$i] )
    {
        # Check if the disks device is disk and not cdrom, we want the disk
        # image path !
        if ( $xml->{'domain'}->[0]->{'devices'}->[0]->{'disk'}->[$i]->{'device'} eq "disk" )
        {
            # Return the file attribute in the source tag
            return $xml->{'domain'}->[0]->{'devices'}->[0]->{'disk'}->[$i]->{'source'}->[0]->{'file'};
        }

        # If it's not the disk disk, try the next one
        $i++;
    }

    return undef;
}

################################################################################
# createEmptyDiskImage
################################################################################
# Description:
#  
################################################################################

sub createEmptyDiskImage
{
    my ( $disk_image, $config_entry , $backing_file ) = @_;

    my $format = getValue( $config_entry, "sstvirtualizationdiskimageformat");

    # Generate the commands to be executed
    my @args = ('qemu-img',
                'create',
                '-f',
                $format,
                '-b',
                $backing_file,
                $disk_image
               );

    # Execute the commands:
    my ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not create empty disk image '$disk_image': "
               ."error: $command_err" );
        return Provisioning::Backup::KVM::Constants::CANNOT_CREATE_EMPTY_DISK_IMAGE;
    }

    # Set correct permission and ownership
    my $owner = getValue( $config_entry, "sstvirtualizationdiskimageowner");
    my $group = getValue( $config_entry, "sstvirtualizationdiskimagegroup");
    my $octal_permission = getValue( $config_entry, "sstvirtualizationdiskimagepermission");

    # Change ownership, generate commands
    @args = ('chown',"$owner:$group",$disk_image);

    # Execute the commands:
    ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not set ownership for disk image '$disk_image':"
               ." error: $command_err" );
        return Provisioning::Backup::KVM::Constants::CANNOT_SET_DISK_IMAGE_OWNERSHIP;
    }

    # Change ownership, generate commands
    @args = ('chmod',$octal_permission,$disk_image);

    # Execute the commands:
    ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not set permission for disk image '$disk_image'"
               .": error: $command_err" );
        return Provisioning::Backup::KVM::Constants::CANNOT_SET_DISK_IMAGE_PERMISSION;
    }


    # if everything is OK we log it and return
    logger("debug","Empty disk image '$disk_image' created");
    return SUCCESS_CODE;
}

################################################################################
# saveXMLDescription
################################################################################
# Description:
#  
################################################################################

sub saveXMLDescription
{
    my ( $machine, $file, $cfg ) = @_;

    # Get the machines XML description using the libvirt api
    my $xml_string;
    eval
    {
        $xml_string = $machine->get_xml_description();
    };

    my $libvirt_err = $@;
               
    # Test if there was an error
    if ( $libvirt_err )
    {
        my $error_message = $libvirt_err->message;
        my $error = $libvirt_err->code;
        logger("error","Error from libvirt (".$error
              ."): libvirt says: $error_message.");
        return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_XML;
    }

    # If everything is alright we can write the xml string to the file
    if ( $dry_run )
    {
        print "DRY-RUN:  $xml_string > $file\n\n";
    } else
    {
        # Test if the directory already exists
        unless ( -d dirname( $file ) )
        {
            # If it does not exist, create it
            if ( createDirectory( dirname( $file, $cfg ) ) != SUCCESS_CODE )
            {
                logger("error","Cannot create directory ".dirname ($file )
                      ."Cannot save the XML file ($file).");
                return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_XML;
            }
        }

        # Open the file and write the XML string to it
        if ( !open(XML, ">$file") )
        {
            # Failed to open the file
            logger("error","Cannot open the file $file for writing: $!. "
                  ."Stopping here!");
            return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_XML;
        } else
        {
            # Write the XML description to the file and close the filehandler
            print XML $xml_string;
            close XML;
        }
    }

    return SUCCESS_CODE;
}


################################################################################
# saveBackendEntry
################################################################################
# Description:
#  
################################################################################

sub saveBackendEntry
{
    my ( $backend_entry, $file, $backend, $cfg ) = @_;

    # Get the machine entry
    my $machine_entry = getParentEntry( getParentEntry( $backend_entry ) );

    # Replace the %backend% with the appropriate backend file suffix
    switch ( $backend )
    {
        case "LDAP" { $file =~ s/%backend%/ldif/; }
        else { $file =~ s/%backend%/export/;}
    }

    # Test if the directory already exists
    unless ( -d dirname( $file ) )
    {
        # If it does not exist, create it
        if ( createDirectory( dirname( $file ), $cfg ) != SUCCESS_CODE )
        {
            logger("error","Cannot create directory ".dirname ($file )
                  ."Cannot save the backend entry ($file).");
            return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_BACKEND_ENTRY;
        }
    }

    # Export the backend entry to the given file
    my $error = exportEntryToFile( $machine_entry, $file );

    # Check if there was an error
    if ( $error )
    {
        # Log it and return
        logger("error","Could not export the backend entry to the file $file"
              ."Stopping here.");
        return Provisioning::Backup::KVM::Constants::CANNOT_SAVE_BACKEND_ENTRY;
    }

    # Otherwise log success and return
    logger("debug","Successfully exported backend entry to $file");
    return SUCCESS_CODE;
}

################################################################################
# createDirectory
################################################################################
# Description:
#  
################################################################################

sub createDirectory
{
    my  ($directory, $cfg) = @_;

    # Check if the parent directory exists, if not we need also to create this 
    # one. So spilt the directory into its parts and remove the last one
    my @parts = split( "/", $directory );
    pop( @parts );

    # The parent directory now is the parts put together again
    my $parent_dir = join( "/", @parts );

    # Test if this directory exists, if not create it
    createDirectory ( $parent_dir, $cfg ) unless ( -d $parent_dir );

    # OK parent directory exists, we can create the actual directory

    # Generate the commands to create the directory
    my @args = ( "mkdir", $directory );

    # Execute the command
    my ($output , $command_err) = executeCommand( $gateway_connection, @args );

    # Check if there was an error
    if ( $command_err )
    {
        # Write log and return error 
        logger("error","Cannot create directory $directory: $output");
        return Provisioning::Backup::KVM::Constants::CANNOT_CREATE_DIRECTORY;
    }

    # If there was no error, change the owenership and the permission
    my $owner = $cfg->val("Directory", "OWNER");
    my $group = $cfg->val("Directory", "GROUP");
    my $permission = $cfg->val("Directory", "OCTAL-PERMISSION");

        # Change ownership, generate commands
    @args = ('chown', "$owner:$group", $directory);

    # Execute the commands:
    ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not set ownership for directory '$directory':"
               ." error: $command_err" );
        return Provisioning::Backup::KVM::Constants::CANNOT_SET_DIRECTORY_OWNERSHIP;
    }

    # Change ownership, generate commands
    @args = ('chmod', $permission, $directory);

    # Execute the commands:
    ($output , $command_err) = executeCommand( $gateway_connection , @args );

    # Test whether or not the command was successfull: 
    if ( $command_err )
    {
        # If there was an error log what happend and return 
        logger("error","Could not set permission for directory '$directory'"
               .": error: $command_err" );
        return Provisioning::Backup::KVM::Constants::CANNOT_SET_DIRECTORY_PERMISSION;
    }

    # Success! Log it and return
    logger("debug", "Directory $directory successfully created");
    return SUCCESS_CODE;

}

################################################################################
# setIntermediatePath
################################################################################
# Description:
#  
################################################################################

sub setIntermediatePath
{

    my $machine = shift;

    # Get the disk image path
    my $image_path = getDiskImageByMachine( $machine );

    # Remove the /var/virtualization in front of the path
    $image_path =~ s/\/var\/virtualization\///;

    # Return now the dirname of the file
    return dirname( $image_path );

}



sub getConfigEntry
{

    my ($entry, $cfg) = @_;

    # Create the var which will be returned and will contain the config entry
    my $config_entry;

    # First of all check if the partent entry is the config entry
    my $parent_entry = getParentEntry( $entry );

    # Check if the parent entry is of objectclass
    # sstVirtualizationBackupObjectClass if yes it is the config entry, if no
    # we need to go the the vm-pool and check if this one is the config entry
    my @objectclass = getValue( $parent_entry, "objectclass" );

    # Go through the array and check for sstVirtualizationBackupObjectClass
    foreach my $value ( @objectclass )
    {
        # If the current value is sstVirtualizationBackupObjectClass then the
        # parent entry is the configuration entry, return it
        if ( $value eq "sstVirtualizationBackupObjectClass" )
        {
            logger("info","Backup configuration is VM specific");
            return $parent_entry;
        }
        
    }

    # At this point, the parent entry is not the config entry, go to the parent
    # entrys parent to get the vm pool
    my $grand_parent_entry = getParentEntry( $parent_entry );

    # Get the vm pool from the grand parent entry
    my $vm_pool_name = getValue( $grand_parent_entry, "sstVirtualMachinePool");

    # Search for the given pool
    # Create the subtree for the pool where the object class would be 
    # sstVirtualizationBackupObjectClass if the pool is the config entry
    my $subtree = "ou=backup,sstVirtualMachinePool=$vm_pool_name,"
                 ."ou=virtual machine pools,";
    $subtree .= $cfg->val("Database","SERVICE_SUBTREE");

    my @entries = simpleSearch( $subtree,
                                "(objectclass=sstVirtualizationBackupObjectClass)",
                                "base"
                              );

    # Test if there are more than one result ( that would be veeery strange )
    if ( @entries > 1 ) 
    {
        # Log and return error
        logger("error","There is something very strange, more than one pool "
              ."with name '$vm_pool_name' found. Cannot return configuration "
              ."entry. Stopping here.");
        return Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY;
    }

    # Otherwise there is one or zero entrys, if there is one it is the config
    # entry so return it
    if ( @entries == 1 )
    {
        logger("info","Backup configuration is VM-Pool specific");
        return $entries[0];
    }

    # If the vm pool did not contain the configuration, we get the foss-cloud 
    # wide backup configuration
    my $global_conf = $cfg->val("Database","FOSS_CLOUD_WIDE_CONFIGURATION");

    # Search this entry with objectclass sstVirtualizationBackupObjectClass
    @entries = simpleSearch( $global_conf,
                             "(objectclass=sstVirtualizationBackupObjectClass)",
                             "base"
                           );
    
    # Test if there are more than one result ( that would be veeery strange )
    if ( @entries > 1 ) 
    {
        # Log and return error
        logger("error","There is something very strange, more than one global "
              ."configuration found. Cannot return configuration "
              ."entry. Stopping here.");
        return Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY;
    } elsif ( @entries == 0 )
    {
        # Log and return error
        logger("error","Global configuartion ($global_conf) not found, cannot "
              ."return configuration entry. Stopping here.");
        return Provisioning::Backup::KVM::Constants::CANNOT_FIND_CONFIGURATION_ENTRY;
    }

    # Or we are lucky and can return the configuration entry which is the global
    # one
    logger("info","Backup configuration is default FOSS-Cloud configuration");
    return $entries[0];


}
################################################################################
# showWait
################################################################################
# Description:
#  
################################################################################

sub showWait
{

    my $times = shift;

    my $i = 0;

    # Avoid endless loops
    return if ( $times < 0 );

    # Avoid to long loops
    return if ( $times > 60);

    # Print the dots....
    while ( $i++ < $times )
    {
        # Print out a point (.) and sleep for one second
        print ". ";
        sleep 1;
    }

    # Nicer look and feel
    print "\n\n";

}

################################################################################
# writeDurationToBackend
################################################################################
# Description:
#  
################################################################################

sub writeDurationToBackend
{

    my ($entry, $type, $duration, $connection) = @_;

    # Get the initial value of the duration:
    my @list = getValue($entry,"sstProvisioningExecutionTime");

    # Check if the element is already in the list
    my $found = 0;

    # Go through the list and change the appropriate value
    foreach my $element (@list)
    {
        if ( $element =~ m/$type/ )
        {
            $element = "$type: $duration";
            $found = 1;
        }
    }

    # If the element was not already in the list, add it
    if ( $found == 0)
    {
        push( @list, "$type: $duration" );
    }

    # Modify the list in the backend
    modifyAttribute( $entry,
                     "sstProvisioningExecutionTime",
                     \@list,
                     $connection
                   );

}

1;

__END__

=pod

=cut
