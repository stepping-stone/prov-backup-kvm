#!/usr/bin/perl -w

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

use warnings;
use strict;

use PerlUtil::LDAPUtil;
use PerlUtil::Logging;
use POSIX;
use Net::LDAP::Entry;
use Config::IniFiles;

my $debug = 1;

# Get the configuration file
my $cfg = new Config::IniFiles(-file=> "/etc/Provisioning/LDAPKVMWrapper.conf");

# Genereate the List of all machines to backup

# First of all connect to the LDAP
my $ldap_server = $cfg->val("LDAP","Server");
my $ldap_connection = LDAPConnect( $ldap_server,
                                   $cfg->val("LDAP","Port"),
                                   $cfg->val("LDAP","Username"),
                                   $cfg->val("LDAP","Password") 
                                  );

# If the connection could not be established log it and exit
unless ( $ldap_connection )
{
    logger("error","Could not create LDAP connection to server $ldap_server. "
          ."Script stopps here!",$debug);
    exit;
}

# Get the current date
my $date = strftime("%Y%m%dT%H%M%SZ",gmtime());

# Get the hostname
my $hostname = $cfg->val("Backup","Hostname") ;

# Get ALL machines on the THIS host
my @all_machines = LDAPSearch("ou=virtual machines,ou=virtualization,"
                             ."ou=services,dc=foss-cloud,dc=org",
                              "sub",
                              "(sstNode=$hostname)",
                              $ldap_connection
                              );

logger("debug","The following machines were found:",$debug);
foreach my $machine (@all_machines)
{
    logger("debug",getValue($machine,"sstVirtualMachine"), $debug );
}

# Test which machine to exculde from backup
my @backup_machines;
foreach my $machine (@all_machines)
{
    # Get the backup subtree, if it does not exists, the machine is new and 
    # needs to be backed up
    my @backup_subtree = LDAPSearch( getValue($machine,"dn"),
                                     "one",
                                     "ou=backup",
                                     $ldap_connection );
    
    # If no backup subtree is present, add it and add the machine to the list 
    # of backup_machines
    if ( @backup_subtree == 0 )
    {         
        next if ( createBackupSubtree($machine,$ldap_connection,$date) );
        push( @backup_machines, getValue($machine, "sstVirtualMachine") );
    } elsif ( @backup_subtree == 1 )
    {
        # Test if the machine is excluded form backup, if yes, do nothing
        if ( getValue($backup_subtree[0],"sstBackupExcludeFromBackup") && 
             getValue($backup_subtree[0],"sstBackupExcludeFromBackup") eq "TRUE"
           )
        {
            logger("info","Machine ".getValue($machine,"sstVirtualMachine")." "
                  ."is excluded from the backup, doing nothing!",$debug);
            next;
        }
        
        # If the machine is not excluded form backup, check if the last backup
        # was successful, if not, do not include it in this run
        my @last_backup = LDAPSearch( getValue($backup_subtree[0],"dn"),
                                      "one",
                                      "(objectClass=*)",
                                      $ldap_connection
                                    );

        # We expect one result
        if ( @last_backup == 1 )
        {
            # Check the return value (should be 0) and the sstProvisionigState
            # (should be "finished")
            if ( getValue($last_backup[0],"sstProvisioningReturnValue") != 0 ||
                 getValue($last_backup[0],"sstProvisioningMode") ne "finished"
               )
            {
                logger("error","The last backup for the machine "
                      .getValue($machine,"sstVirtualMachine")." was not success"
                      ."full. Skipping this machine!",1
                      );
                next;
            }
        } else
        {
            logger("warning","Have ".@last_backup." last backups for machine "
                  .getValue($machine,"sstVirtualMachine").". Something is wrong"
                  ." here! Skipping this machine!",$debug);
            next;
        }
        
        logger("info","Processing machine "
              .getValue($machine, "sstVirtualMachine"),$debug);
        
        # If everything is fine, adjust the backup subtree and add the machine 
        # to the backup machines list
        next if (createBackupSubtree($machine,$ldap_connection,$date,"update"));
        push( @backup_machines, getValue($machine, "sstVirtualMachine") );
    } else
    {
        # More than one backup subtrees found! Very strange!
        logger("error","Found ".@backup_subtree." backup subtrees for machine "
              .getValue($machine,"sstVirtualMachine")."! This is very strange "
              ."please fix this issue as fast as possible! The machine will "
              ."not be backed up!",$debug);
    }
    
}

# Finally generate the command to backup all machines
my $pass_debug = $debug ? "-d" : "";
my $script = $cfg->val("Backup","Script");
my $configuration = $cfg->val("Backup","Configuration");
my $command = "$script $pass_debug -c $configuration -i \"".
              join(",",@backup_machines)."\"";

# Execute the command and wait until the wrapper finished his job!
system($command);

# Replace the sstProvisioningMode "retained" in all machines with "finished"
foreach my $to_replace ( @backup_machines )
{
    
    # We know that for this machine the backup subtree with the date leaf exists
    # so we can directly search this entry
    my @machine_object = LDAPSearch("ou=$date,ou=backup,sstVirtualMachine="
                                   ."$to_replace,ou=virtual machines,ou="
                                   ."virtualization,ou=services,dc=foss-cloud,"
                                   ."dc=org",
                                   "base",
                                   "(objectClass=sstProvisioning)",
                                   $ldap_connection
                                   );

    # If the entry could not be found something is wrong!
    if ( @machine_object != 1 )
    {
        logger("error","Could not find the backup leaf for machine $to_replace."
              ." This is very strange since it has been created earlier! Plase "
              ."have a look at it and replace the 'retained' with 'finished' in"
              ."the sstProvisioningMode attribute!");
        next;
    }
    
    # If the sstProvisioningMode is retained, and sstProvisioningReturnValue is
    # 0, we can replace 'retained' with 'finished'
    if ( getValue($machine_object[0],"sstProvisioningReturnValue") == 0 && 
         getValue($machine_object[0],"sstProvisioningMode") eq "retained" )
    {
        # Log that everything is ok
        logger("debug","Backup for machine $to_replace was successful, "
              ."replacing 'retained' with 'finished'");
              
        my $result = modifyAttribute( $machine_object[0],
                                      "sstProvisioningMode",
                                      "finished",
                                      $ldap_connection
                                    );
                                    
        if ( $result )
        {
            logger("warning","Could not replace the sstProvisioningMode "
                  ."attribute with finished for machine $to_replace! Please "
                  ." replace it manually (on server $ldap_server)!");
        }
    } else
    {
        logger("warning","Backup for machine $to_replace was not successful."
              ." Will not modify the sstProvisioningMode attribute!");
    }
}

sub createBackupSubtree
{
    my $machine = shift;
    my $ldap_connection = shift;
    my $date = shift;
    my $update = shift;
    
    # Log what we are doing
    logger("debug","Creating backup subtree for machine "
          .getValue($machine, "sstVirtualMachine"),$debug);
    
    # Create the backup entry leaf
    my $today_entry = Net::LDAP::Entry->new;
    
    # Calculate the dn
    my $dn = "ou=$date,ou=backup,".getValue($machine,"dn");
    
    # Set the dn and the other attributes
    $today_entry->dn($dn);
    $today_entry->add(
                        objectClass => 'organizationalUnit',
                        objectClass => 'sstProvisioning',
                        objectClass => 'top',
                        ou => $date,
                        sstProvisioningExecutionDate => 0,
                        sstProvisioningMode => 'snapshot',
                        sstProvisioningState => 0,
                        sstProvisioningExecutionTime => 'snapshot: 0',
                        sstProvisioningExecutionTime => 'merge: 0',
                        sstProvisioningExecutionTime => 'retain: 0',
                        sstProvisioningReturnValue => 0,
                     );

    # If it is not an update, create the backup entry and add it to the LDAP
    unless ( $update )
    {
        $dn =~ s/ou=$date,//;
        my $backup_entry = Net::LDAP::Entry->new;
        $backup_entry->dn($dn);
        $backup_entry->add(
                            objectClass => 'organizationalUnit',
                            objectClass => 'top',
                            ou => 'backup',
                          );
        
        # Add the backup entry
        my $result = $ldap_connection->add($backup_entry);
        
        # Check for errors
        if ( $result->code )
        {
            logger("error","Could not add the backup subtree entry $dn for the "
                  ."machine ".getValue($machine,"sstVirtualMachine")."! Cannot "
                  ."continue with the backup process.",$debug);
            return 1;
        }
    } else
    {
        # If it is an update, delete the old entry
        my @old_objects = LDAPSearch("ou=backup,".getValue($machine,"dn"),
                                          "sub",
                                          "(objectClass=sstProvisioning)",
                                          $ldap_connection);
        # We expect exactly one result
        if ( @old_objects > 1 )
        {
            # If its more or less than one
            logger("warning","Found",@old_objects," old backup entries. "
                  ."Will delete all of them!!",$debug);
           
            # Delete every object
            foreach my $old_object ( @old_objects )
            {
                # Delete it
                my $result=$ldap_connection->delete(getValue($old_object,"dn"));
                
                # Check for success
                if ( $result->code )
                {
                    logger("error","Could not remove "
                          .getValue($old_object,"dn").": ".$result->error
                          ." Cannot backup machine "
                          .getValue($machine,"sstVirtualMachine")."!!",$debug);
                    return 1;
                }
               
           }
        } elsif( @old_objects == 1 )
        {
            # Delete the old entry
            my $result = $ldap_connection->delete(getValue($old_objects[0],
                                                  "dn" ) );

            # If there was an error log it and return
            if ( $result->code )
            {
                logger("error","Could not remove "
                      .getValue($old_objects[0],"dn").": ".$result->error
                      ." Cannot backup machine "
                      .getValue($machine,"sstVirtualMachine")."!!",$debug);
                return 1;
            }
        } else
        {
            # No old backup entries are found, this is strange! But anyway, just
            # log it and continue
            logger("warning","Should update backup entry for machine "
                  .getValue($machine,"sstVirtualMachine")." but there is no "
                  ." backup entry present! Creating new one!",$debug);
        }
    }

    # Now we can add the backup entry!
    my $result = $ldap_connection->add($today_entry);
    
    # Check for errors
    if ( $result->code )
    {
        logger("error","Could not add the backup entry for the machine "
              .getValue($machine,"sstVirtualMachine")."! Cannot continue "
              ."with the backup process.",$debug);
        return 1;
    }

}