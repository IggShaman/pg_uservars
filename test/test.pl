#!/usr/bin/perl -w
use strict;
use warnings;

#
# Tester script pre-reqs:
#
# 1. install pg_uservars extension
# 2. createdb test1
# 3. install pg_uservars to "test1" db with e.g.: psql -f pg_uservars.sql test1
#
# This script generates 1000 random small keys and unlimited number of values of up to 10kb in size,
# and does 1m random operations such as: add new key, change value for existing key, remove existing key,
# remove random key. A local hash with keys and values is maintained and checked in getvar(..).
#
# Afterwards, it prints "ps" so one could check memory usage by pgsql backend.
#

use IO::Handle;
autoflush STDOUT 1;

use DBI;
use DBD::Pg;

my $dbh = eval { DBI->connect ( 'dbi:Pg:dbname=test1', '', '',
                                { RaiseError => 1,
                                  AutoCommit => 1,
                                  #PrintError => 1,
                                } ); };
die "ERROR: could not connect to db: $@" if $@;


# hash with vars
# undef value means variable was destroyed
my $vars = {};

sub getvar {
    my $key = $_[0];
    
    my ($val) = $dbh->selectrow_array ( 'select pguser_getvar(?)', undef, $key );
    #print "getvar \"$key\" => ", (defined $val ? '"' . $val . '"' : '<undef>'), "\n";
    
    if ( defined $val ) {
	die "\$vars: key=\"$key\" no value for key=\"$key\"\n" unless exists $vars->{$key} and defined $vars->{$key};
	die "\$vars: key=\"$key\": value \"$vars->{$key}\" != db=\"$val\"\n" unless $vars->{$key} eq $val;
    } else {
	die "\$vars: key=\"$key\": have value=\"$vars->{$key}\", db hasn't\n" if exists $vars->{$key} and defined $vars->{$key};
    }
    
    return $val;
}

sub setvar {
    my ($key, $val) = @_;
    $dbh->selectrow_array ( 'select pguser_setvar(?,?)', undef, $key, $val );
    #print "setvar \"$key\" => ", (defined $val ? '"' . $val . '"' : '<undef>'), "\n";
    $vars->{$key} = $val;
}


sub delvar {
    my ($key) = @_;
    $dbh->selectrow_array ( 'select pguser_delvar(?)', undef, $key );
    #print "delvar \"$key\"\n";
    $vars->{$key} = undef;
}

sub randstr {
    my $sz = 3 + int rand($_[0] // 3);
    my @rv;
    while ( $sz-- ) {
	push @rv, chr(ord('a') + int rand(26));
    }
    my $rv = join ( '', @rv );
    #print "randstr \"$rv\"\n";
    return $rv;
}

my @rand_names;
for ( 1..1000 ) {
    push @rand_names, randstr();
}
my $rand_names_nr = scalar(@rand_names);

sub randname {
    return $rand_names[int rand($rand_names_nr)];
}


my $nr = 0;
print "Progress: [";
while ( $nr++ < 1000000 ) {
    
    print '.' if $nr % 1000 == 0;
    print ' ', $nr, ' ' if $nr % 100000 == 0;
    
    my $op = int rand(3);
    
    if ( 0 == $op ) {      # add random value
	setvar ( randname(), randstr(10240) );
	
    } elsif ( 1 == $op ) { # change existing value
	my @k = keys %$vars;
	next unless @k;
	my $i = int(rand(scalar(@k)));
	my $key = $k[$i];
	unless ( defined $key and length $key ) {
	    die "UNDEFINED KEY! rand i=$i len=", scalar(@k), "\n";
	}
	setvar ( $key, randstr() );
	
    } elsif ( 2 == $op ) { # remove existing value
	my @k = keys %$vars;
	next unless @k;
	my $i = int(rand(scalar(@k)));
	my $key = $k[$i];
	unless ( defined $key and length $key ) {
	    die "UNDEFINED KEY! rand i=$i len=", scalar(@k), "\n";
	}
	
	delvar ( $key ) if defined $key;
	
    } elsif ( 3 == $op ) { # del random value
	delvar ( randname() );
    }
}
print "]\n";

#
# check all
#
my $tot_nr = 0;
my $set_nr = 0;
while ( my ($k, $v) = each %$vars ) {
    #note: getvar does all checks
    my $v = getvar ( $k );
    
    $tot_nr++;
    $set_nr++ if defined $v;
}

delvar ( $_ ) foreach keys %$vars;

print "total entr(ies): $tot_nr\n";
print "  set entr(ies): $set_nr\n";

#
# print memory usage while still connected
#
print `ps auxww|grep postgres`;

#
# disconnect from db and let it free up some memory
#
$dbh->disconnect();
sleep 2;

#
# print memory usage after disconnect
#
print `ps auxww|grep postgres`;

1;
