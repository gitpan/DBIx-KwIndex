#!perl
use ExtUtils::MakeMaker qw(prompt WriteMakefile);
use Getopt::Long;
use Data::Dumper;

$options = { prompt=>1 };

Getopt::Long::GetOptions($options, 
                         'test-dsn=s',
                         'test-user=s',
                         'test-pass=s',
                         'test-index-name=s',
                         'help','prompt!');

if ($options->{'help'}) {
	print "
Usage: perl Makefile.PL <options>

Possible Options are:

  --test-dsn=dsn            Sets data source, user name, and password 
  --test-user=user          to connect to database.
  --test-pass=pass
  --test-index-name         Sets index name to create for make test.

  --noprompt                Disables interactive prompting.

";
exit 0;
}

$prompt = $options->{prompt};

$configfile = 'test/Config.pl';
mkdir 'test', 0755;

if (-e $configfile) {
	do $configfile;
}

$config ||= {};

$reuse = 'n';
if (keys %$config) {
	$reuse = 'y';
	$reuse = prompt('Use previous configuration for tests(y/n)?', $reuse) if $prompt;
}

if ($reuse !~ /^[Yy]$/) {
	print "For running `make test' it is necessary to have a database to connect to\n";
	print "Please enter a valid datasource, username, and password.\n\n";

	$test_dsn  = exists($options->{'test-dsn'}) ? $options->{'test-dsn'} : exists($config->{'test-dsn'}) ? $config->{'test-dsn'} : 'dbi:mysql:test';
	$test_dsn = prompt('Datasource for connecting to database?', $test_dsn) if $prompt;
	$test_dsn = undef if $test_dsn eq '';

	$test_user = exists($options->{'test-user'}) ? $options->{'test-user'} : exists($config->{'test-user'}) ? $config->{'test-user'} : 'test';
	$test_user = prompt ('User name for connecting to database?', $test_user) if $prompt;
	$test_user = undef if $test_user eq '';

	$test_pass = exists($options->{'test-pass'}) ? $options->{'test-pass'} : exists($config->{'test-pass'}) ? $config->{'test-pass'} : 'test';
	$test_pass = prompt('Password for connecting to database?', $test_pass) if $prompt;
	$test_pass = undef if $test_pass eq '';

	$test_index_name = exists($options->{'test-index-name'}) ? $options->{'test-index-name'} : exists($config->{'test-index-name'}) ? $config->{'test-index-name'} : 'kwindex';
	$test_index_name = prompt('Index name?', $test_index_name) if $prompt;
	$test_index_name = undef if $test_index_name eq '';

	$config->{'test-dsn'}  = $test_dsn;
	$config->{'test-user'} = $test_user;
	$config->{'test-pass'} = $test_pass;
	$config->{'test-index-name'} = $test_index_name;
}

open CF, ">$configfile" or die $!;
print CF Data::Dumper->Dump([$config],['$config']);
close CF or die $!;

WriteMakefile(
    'NAME' => 'DBIx::KwIndex',
    'VERSION_FROM' => 'KwIndex.pm', # finds $VERSION
	'PREREQ_PM' => { 'DBI' => 0 },
	'realclean' => { FILES => './test' },
);
