# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { $| = 1; print "1..14\n"; }
END {print "not ok 1\n" unless $loaded;}
use DBIx::KwIndex;
$loaded = 1;
print "ok 1\n";

######################### End of black magic.

# Insert your test code below (better if it prints "ok 13"
# (correspondingly "not ok 13") depending on the success of chunk 13
# of the test code):

$configfile = 'test/Config.pl';
do $configfile;

package MyKwIndex;
use base 'DBIx::KwIndex';
# text from Project Gutenberg Etext of Anomalies and Curiosities of Medicine
# by George M. Gould and Walter L. Pyle

$docs = [
'',

q(Since the time when man's mind first busied itself with subjects
beyond his own self-preservation and the satisfaction of his
bodily appetites, the anomalous and curious have been of
exceptional and persistent fascination to him; and especially is
this true of the construction and functions of the human body.),

q(In the older works, the following authors have reported cases of
pregnancy before the appearance of menstruation: Ballonius,
Vogel, Morgagni, the anatomist of the kidney, Schenck,
Bartholinus, Bierling, Zacchias, Charleton, Mauriceau,
Ephemerides, and Fabricius Hildanus.),

q((1) The imagination-theory, or, to quote Harvey: "Due to mental
causes so operating either on the mind of the female and so
acting on her reproductive powers, or on the mind of the male
parent, and so influencing the qualities of his semen, as to
modify the nutrition and development of the offspring."),

q(Wygodzky finds that the greatest number of coils of the umbilical
cord ever found to encircle a fetus are 7 (Baudelocque), 8
(Crede), and 9 (Muller and Gray). His own case was observed this
year in Wilna. The patient was a primipara aged twenty. The last
period was seen on May 10, 1894. On February 19th the fetal
movements suddenly ceased. On the 20th pains set in about two
weeks before term.),

q(Warren gives an instance of a lady, Mrs. M----, thirty-two years
of age, married at fourteen, who, after the death of her first
child, bore twins, one living a month and the other six weeks.
Later she again bore twins, both of whom died. She then
miscarried with triplets, and afterward gave birth to 12 living
children, as follows: July 24, 1858, 1 child; June 30, 1859, 2
children; March 24, 1860, 2 children; March 1, 1861, 3 children;
February 13, 1862, 4 children; making a total of 21 children in
eighteen years, with remarkable prolificity in the later
pregnancies. She was never confined to her bed more than three
days, and the children were all healthy.)
];


sub document_sub {
	my ($self, $ids) = @_;
	[@{$docs}[@$ids]];
}

package main;


use DBI;
if ($dbh = DBI->connect($config->{'test-dsn'}, $config->{'test-user'}, $config->{'test-pass'})) {
	print "ok 2\n";
} else {
	print "not ok 2\n";
	exit 1;
}


eval { $kw = MyKwIndex->new({dbh => $dbh, index_name => $config->{'test-index-name'}}) };
if ($kw and !$@) {
	print "ok 3\n";
} else {
	print "not ok 3".($@ ? ": $@":'')."\n";
	exit 1;
}


if ($kw->empty_index) {
	print "ok 4\n";
} else {
	print "not ok 4\n";
	exit 1;
}

print (($kw->add_document([1]) ? "ok":"not ok") . " 5\n");
print (($kw->document_count == 1 && $kw->word_count == 38 ? "ok":"not ok") . " 6\n");
$kw->add_document([2..5]);
print (($kw->document_count == 5 && $kw->word_count == 189 ? "ok":"not ok") . " 7\n");
print (($kw->remove_document([5]) ? "ok":"not ok") . " 8\n");
print (($kw->document_count == 4 ? "ok":"not ok") . " 9\n");

$MyKwIndex::docs->[5] = 
q(According to a French authority the wife of a medical man at
Fuentemajor, in Spain, forty-three years of age, was delivered of
triplets 13 times. Puech read a paper before the French Academy
in which he reports 1262 twin births in Nimes from 1790 to 1875,
and states that of the whole number in 48 cases the twins were
duplicated, and in 2 cases thrice repeated, and in one case 4
times repeated.);

print (($kw->update_document([5]) ? "ok":"not ok") . " 10\n");
print (($kw->document_count == 5 && $kw->word_count == 216 ? "ok":"not ok") . " 11\n");

print ((($stop_words = $kw->common_word) && @$stop_words == 4 ? "ok":"not ok") . " 12\n");

$kw->empty_index;
$kw->add_document([1..5]); 
print (($kw->document_count == 5 && $kw->word_count == 168 ? "ok":"not ok") . " 13\n");

@r = map { $kw->match_count({words=>$_,boolean=>'AND'}) } 'from and', 'from and the', 'from or and the';
if ($r[0]!=1 || $r[1]!=1 || $r[2]!=0) { print "not " }; print "ok 14\n";

@r = map { $kw->match_count({words=>$_,boolean=>'OR'}) } 'from and', 'from and the', 'from or and the';
if ($r[0]!=5 || $r[1]!=5 || $r[2]!=5) { print "not " }; print "ok 15\n";

print (($kw->remove_index ? "ok":"not ok") . " 16\n");

