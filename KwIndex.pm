package DBIx::KwIndex;

use strict;
use Carp;
use vars qw($VERSION $ME $debug);

$VERSION = 0.02;
$ME      = 'DBIx::KwIndex';

$debug = 0;
sub _debug { return unless $debug; print "debug: ", @_, "\n"; }

# CONSTRUCTOR
#############

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = shift || {};

    croak "$ME: new: first arg must be a hashref" if ref($self) ne 'HASH';
    croak "$ME: new: missing arg: 'dbh'" if !exists $self->{'dbh'};
    
    $self = bless $self, $class;

    for ($self->{'index_name'})             {$_ = 'kwindex'   if !defined}
    for ($self->{'wordlist_cardinality'})   {$_ = 100_000     if !defined}
    for ($self->{'stoplist_cardinality'})   {$_ = 10_000      if !defined}
    for ($self->{'vectorlist_cardinality'}) {$_ = 100_000_000 if !defined}
    for ($self->{'doclist_cardinality'})    {$_ = 1_000_000   if !defined}
    for ($self->{'max_word_length'})        {$_ = 32          if !defined}

    if (!$self->_index_exists) { $self->_create_index }

    # prepare statements
    my $dbh = $self->{'dbh'};
    my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	
    $self->{'_sth_add_doclist'} = 
      $dbh->prepare("REPLACE INTO ${idx}_doclist (id,n) VALUES (?,?)") or
      croak "$ME: new: Can't prepare statement: ".$dbh->errstr;
    $self->{'_sth_add_wordlist'} = 
      $dbh->prepare("INSERT IGNORE INTO ${idx}_wordlist (word) VALUES (?)") or
      croak "$ME: new: Can't prepare statement: ".$dbh->errstr;
    $self->{'_sth_add_stoplist'} = 
      $dbh->prepare("REPLACE INTO ${idx}_stoplist (word) VALUES (?)") or
      croak "$ME: new: Can't prepare statement: ".$dbh->errstr;
    $self->{'_sth_add_vectorlist'} = 
      $dbh->prepare("REPLACE INTO ${idx}_vectorlist (wid,did,f) VALUES (?,?,?)") or
      croak "$ME: new: Can't prepare statement: ".$dbh->errstr;
    $self->{'_sth_add_stoplist'} = 
      $dbh->prepare("REPLACE INTO ${idx}_stoplist (word) VALUES (?)") or
      croak "$ME: new: Can't prepare statement: ".$dbh->errstr;

    # load stoplist in a hash
	my $stoplist = {};
	my $w;
	my $sth1 = $dbh->prepare("SELECT word FROM ${idx}_stoplist") or
	  croak "$ME: new: Can't load stoplist: ".$dbh->errstr;
	$sth1->execute() or
	  croak "$ME: new: Can't load stoplist: ".$dbh->errstr;
	while (($w) = $sth1->fetchrow_array) { $stoplist->{$w}=1 }
	$self->{'stoplist'} = $stoplist;
	
    $self;
}


# DESTRUCTOR
############

sub DESTROY {
    my $self = shift;
    for (qw(_sth_add_doclist 
      _sth_add_wordlist
      _sth_add_stoplist
      _sth_add_vectorlist
      _sth_add_stoplist)) {
        $self->{$_}->finish;
    }
    
}


# PUBLIC METHODS
################

# module user has to override this method
sub document_sub { croak "$ME: document_sub: this method must be overriden" }

sub add_document {
    my $self     = shift;
    my $doc_ids  = shift or croak "$ME: add_document: \$kw->add_document([id,...])";
	croak "$ME: add_document: arg 1 must be an ARRAY ref" unless ref($doc_ids) eq 'ARRAY';
	
    my %wordlist = (); # format: ( 'word1' => [ [doc_id,freq], ... ], ... )
    my %doclist  = (); # format: ( doc_id => n, ... ); # n = number of words in document
    
    # retrieve documents
    ####################
    my $docs = $self->document_sub($doc_ids);

    croak "$ME: add_document: 'document_sub' does not return ARRAY ref"
      unless ref($docs) eq 'ARRAY';
    croak "$ME: add_document: 'document_sub' does not return enough documents"
      if $#{$doc_ids} > $#{$docs}; 
    croak "$ME: add_document: 'document_sub' returns too many documents"
      if $#{$doc_ids} < $#{$docs}; 
    
	my $stoplist = $self->{'stoplist'};
	
    # split documents into words
    ############################
    for my $i (0..$#{$doc_ids}) {
    	for ($docs->[$i]) { next unless defined and length }
    	
        my $w = _split_to_words($docs->[$i]);
        my $num_of_words = @$w; # note: this means that numbers, etc are counted
        $doclist{$doc_ids->[$i]} = $num_of_words;

        my %w;
        for (@$w) {
            # skip non-qualifying words: 1-char length, numbers,
            # words that are too long
            next if length($_) == 1
                or $_ !~ /[A-Za-z]/
                or length($_) > $self->{'max_word_length'};

            # skip stop words
			next if exists $stoplist->{lc($_)};

            $w{lc($_)}++;
        }
        
        for (keys %w) {
            push @{ $wordlist{$_} }, [ $doc_ids->[$i], $w{$_}/$num_of_words ];
        }
    }

    # submit to database
    ####################
    my $dbh = $self->{'dbh'};
    my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	    
    # lock the tables in case some other process remove a certain word
    # between step 0 and 1 and 2 and 3
    $dbh->do("LOCK TABLES ${idx}_doclist WRITE, ${idx}_vectorlist WRITE, ${idx}_wordlist WRITE") or 
      do {$self->{'ERROR'}="Can't lock tables when adding documents: ".$dbh->errstr; return undef};

    # 0
    # add the docs first
    my $sth0 = $self->{'_sth_add_doclist'};
    for (keys %doclist) {
        $sth0->execute($_, $doclist{$_}) or
          do {$self->{'ERROR'}="Can't add to doclist: ".$sth0->errstr; $dbh->do('UNLOCK TABLES'); return undef};
    }
    
    # 1
    # and then add the words 
    my $sth1 = $self->{'_sth_add_wordlist'};
    for (keys %wordlist) {
        $sth1->execute($_) or 
          do {$self->{'ERROR'}="Can't add to wordlist: ".$sth1->errstr; $dbh->do('UNLOCK TABLES'); return undef};
    }

    # 2
    # get the resulting word ids
    my %word_ids = ();
    my $result = 
    $dbh->selectall_arrayref(
    "SELECT id,word FROM ${idx}_wordlist WHERE word IN (".
      join(',',map {$dbh->quote($_)} keys %wordlist).")") or
      do {$self->{'ERROR'}="Can't get data from wordlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef};

    for (@$result) { $word_ids{$_->[1]} = $_->[0] }

    # 3
    # now add the vectors
    my $sth2 = $self->{'_sth_add_vectorlist'};
    for my $w (keys %wordlist) {
        for (@{ $wordlist{$w} }) {
            $sth2->execute($word_ids{$w}, $_->[0], $_->[1]) or
              do {$self->{'ERROR'}="Can't add to vectorlist: ".$sth2->errstr; $dbh->do('UNLOCK TABLES'); return undef};
        }
    }
    
    # if all goes well, return a TRUE value
    $dbh->do('UNLOCK TABLES');
    1;
}

sub remove_document {
    my $self = shift;
    my $doc_ids = shift;

    croak "$ME: remove_document: arg 1 must be an ARRAY ref" unless ref($doc_ids) eq 'ARRAY';

    my $dbh = $self->{'dbh'};
    my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};

    $dbh->do("LOCK TABLES ${idx}_doclist WRITE, ${idx}_wordlist WRITE, ${idx}_vectorlist WRITE") or 
      do {$self->{'ERROR'}="Can't lock tables when removing documents: ".$dbh->errstr; return undef};
    
    $dbh->do("DELETE FROM ${idx}_doclist WHERE id IN (".join(',',@$doc_ids).")") or
      do {$self->{'ERROR'}="Can't delete from vectorlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef}; 
    $dbh->do("DELETE FROM ${idx}_vectorlist WHERE did IN (".join(',',@$doc_ids).")") or
      do {$self->{'ERROR'}="Can't delete from vectorlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef}; 

    # if all goes well, return a TRUE value
    $dbh->do("UNLOCK TABLES");
    1;
}

sub update_document {
    my $self = shift;
    $self->remove_document($_[0]), $self->add_document($_[0]);
}

# find all words that are contained in at least $k % of all documents
sub common_word {
	my $self = shift;
	my $k = shift || 80;
	my $dbh = $self->{'dbh'};
	my $idx = $self->{'index_name'};
	
	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};

	# first select the number of documents
	my $num = $self->document_count;
	defined($num) or do{$self->{'ERROR'}="Can't retrieve the number of documents: ".$dbh->errstr; return undef};
	
	# get the statistics from vectorlist
	my $result1 = $dbh->selectall_arrayref("SELECT wid,COUNT(*)/$num as k FROM ${idx}_vectorlist GROUP BY wid HAVING k>=".($k/100));
	defined($result1) or do{$self->{'ERROR'}="Can't retrieve common words: ".$dbh->errstr; return undef};
	
	# convert it to word by consulting the wordlist table
	my $result2 = $dbh->selectall_arrayref("SELECT word FROM ${idx}_wordlist WHERE id IN (".join(',',map { $_->[0] } @$result1).")");
	defined($result2) or do{$self->{'ERROR'}="Can't retrieve common words: ".$dbh->errstr; return undef};
	
	return [ map { $_->[0] } @$result2 ];
}

# find all words that are not contained in all documents (vectorlist)
# XXX not yet written
sub orphan_word { return [] }

# remove words from index
sub remove_word {
	my $self = shift;
	my $words = shift;
	croak "$ME: remove_word: arg 1 must be an ARRAY ref" unless ref($words) eq 'ARRAY';

	my $dbh = $self->{'dbh'};
	my $idx = $self->{'index_name'};
	
	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	
	$dbh->do("LOCK TABLES ${idx}_wordlist WRITE, ${idx}_vectorlist WRITE");
	# retrieve word ids
	my $result0 = $dbh->selectall_arrayref("SELECT id FROM ${idx}_wordlist WHERE word IN (".join(',',map { $dbh->quote(lc($_)) } @$words).")");
	defined($result0) or do {$self->{'ERROR'}="Can't delete from wordlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef};
	my $word_ids = join(',', map {$_->[0]} @$result0);
	return 1 if !length($word_ids);
	
	# delete from wordlist
	$dbh->do("DELETE FROM ${idx}_wordlist WHERE id IN ($word_ids)") or
	  do {$self->{'ERROR'}="Can't delete from wordlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef};
	$dbh->do("DELETE FROM ${idx}_vectorlist WHERE wid IN ($word_ids)") or
	  do {$self->{'ERROR'}="Can't delete from vectorlist: ".$dbh->errstr; $dbh->do('UNLOCK TABLES'); return undef};
	
	return 1;
}

# add stop words. note: you must manually delete previously indexed words
# with delete_word()
sub add_stop_word {
	my $self = shift;
	my $words = shift;
	croak "$ME: remove_word: arg 1 must be an ARRAY ref" unless ref($words) eq 'ARRAY';

	my $dbh = $self->{'dbh'};
	my $idx = $self->{'index_name'};
	
	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	
	my $stoplist = $self->{'stoplist'};
	my $sth1 = $self->{'_sth_add_stoplist'};
	for (map { lc($_) } @$words) {
		$sth1->execute($_) or
		  do{$self->{'ERROR'}="Can't add to stoplist: ".$dbh->errstr; return undef};
		$stoplist->{$_}=1;
	}
	
	return 1;
}

# remove stop words from index
sub remove_stop_word {
	my $self = shift;
	my $words = shift;
	croak "$ME: remove_word: arg 1 must be an ARRAY ref" unless ref($words) eq 'ARRAY';

	my $dbh = $self->{'dbh'};
	my $idx = $self->{'index_name'};
	
	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	
	defined($dbh->selectall_arrayref("DELETE FROM ${idx}_stoplist WHERE word IN (".join(',',map { $dbh->quote(lc($_)) } @$words).")")) or
	  do {$self->{'ERROR'}="Can't delete from stoplist: ".$dbh->errstr; return undef};
	my $stoplist = $self->{'stoplist'};
	for (@$words) { delete $stoplist->{lc($_)} }  
	
	return 1;
}

sub is_stop_word {
	exists shift->{'stoplist'}->{ lc($_[0]) }
}

sub _search_or_match_count {
	my $is_count = shift;
	my $self = shift;
	my $args = shift || {};

	croak "$ME: search: arg 1 must be a HASH ref" unless ref($args) eq 'HASH';
	croak "$ME: search: option 'words' not defined" unless exists $args->{'words'};

	my $dbh = $self->{'dbh'};
	my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	    
	# split the words if we are offered a SCALAR (assume it's a phrase)
	my $words;
	if (ref($args->{'words'}) eq 'ARRAY') {
		$words = $args->{'words'};
	} else {
		$words = _split_to_words($args->{'words'});
	}

	# delete duplicate words, convert them all to lowercase
	$words = [  keys %{ { map { lc($_) => 1 } @$words } }  ];
	return($is_count ? 0 : []) unless @$words;

    # first we retrieve the word ids
    my $op = $args->{'re'} ? 'REGEXP':'LIKE';
	my $bool = exists($args->{'boolean'}) && defined($args->{'boolean'}) && uc($args->{'boolean'}) eq 'AND' ? 'AND':'OR';

	my $result0 = $dbh->selectall_arrayref("SELECT id FROM ${idx}_wordlist WHERE ".join(' OR ', map {"word $op ".$dbh->quote($_)} @$words)) or
	  do {$self->{'ERROR'}="Can't retrieve word ids: ".$dbh->errstr; return undef};

	my @word_ids = map { $_->[0] } @$result0;
	if (!@word_ids or ($bool eq 'AND' && @word_ids < @$words)) {
		return($is_count ? 0 : []);
    }

	# and then we search the vectorlist
	my $can_optimize=0;
	my $stmt;

	if ($is_count) {

		if ($bool eq 'AND' && !$args->{'re'}) {
			$stmt = 'SELECT did,count(wid) as c '.
			        "FROM ${idx}_vectorlist WHERE wid IN (".join(',',@word_ids).") ".
			        "GROUP BY did ".
			        "HAVING c >= ".scalar(@word_ids);
		} else {
			$can_optimize=1;
			$stmt = "SELECT COUNT(DISTINCT did) ".
			        "FROM ${idx}_vectorlist WHERE wid IN (".join(',',@word_ids).")";
		}

	} else {

		$stmt = 'SELECT did,count(wid) as c,avg(f) as a,count(wid)*count(wid)*count(wid)*avg(f) as ca '.
		        "FROM ${idx}_vectorlist WHERE wid IN (".join(',',@word_ids).") ".
		        "GROUP BY did ".
		        ($bool eq 'AND' && !$args->{'re'} ? 
		        "HAVING c >= ".scalar(@word_ids):'').
		        " ORDER BY ca DESC ".
		        (defined($args->{'num'}) ? "LIMIT " . (defined($args->{'start'}) ? 
		        (($args->{'start'} - 1).",".$args->{'num'}) : $args->{'num'})
		        :'');

	}

	_debug "search SQL: ", $stmt;

    my $result;
	if ($is_count) {
		if ($can_optimize) {
			defined (($result) = $dbh->selectrow_array($stmt)) or
			  do {$self->{'ERROR'}="Can't search vectorlist: ".$dbh->errstr; return undef};
			return $result;
		} else {
			my $sth = $dbh->prepare($stmt);
			my $count=0;
			my @row;
			$sth->execute() or 
			  do {$self->{'ERROR'}="Can't search vectorlist: ".$sth->errstr; return undef};
			++$count while(@row = $sth->fetchrow_array());
			$sth->finish;
			return $count;
		}
	} else {
		$result = $dbh->selectall_arrayref($stmt) or 
    	  do {$self->{'ERROR'}="Can't search vectorlist: ".$dbh->errstr; return undef};
    	return [ map { $_->[0] } @$result ];
	}
}

sub search { _search_or_match_count(0, @_) }

sub match_count { _search_or_match_count(1, @_) }

sub remove_index {
    my $self = shift;
    my $dbh  = $self->{'dbh'};
    my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	    
    for ($self->_index_tables) {
   		$dbh->do("DROP TABLE IF EXISTS $_") or 
		  do { $self->{ERROR} = "Can't remove index table table: $dbh->errstr"; return undef };
    }
    
    1;
}

sub empty_index {
	my $self = shift;

	$self->remove_index && $self->_create_index;

	1;
}

# number of documents in the collection
sub document_count {
    my $self = shift;
    my $dbh  = $self->{'dbh'};
    my $idx = $self->{'index_name'};
	
	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	    
   	my ($num) = ($dbh->selectrow_array("SELECT COUNT(*) FROM ${idx}_doclist"));
   	$num;
}

# number of unique words 
sub word_count {
    my $self = shift;
    my $dbh  = $self->{'dbh'};
    my $idx = $self->{'index_name'};

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	    
   	my ($num) = ($dbh->selectrow_array("SELECT COUNT(*) FROM ${idx}_wordlist"));
   	$num;
}


# PRIVATE METHODS
#################

sub _split_to_words {
    return [ $_[0] =~ /\b(\w[\w']*\w+|\w+)\b/g ];
}

sub _create_index {
    my $self = shift;
    my $dbh = $self->{'dbh'};
    my $idx = $self->{'index_name'};
    my $stmt;

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};
	
    # drop previous tables, if they exist
    $self->remove_index;

    # create doclist table
    $stmt = "CREATE TABLE ${idx}_doclist " . 
      '(id ' . _int_column_type($self->{'doclist_cardinality'}) . 
        ' AUTO_INCREMENT PRIMARY KEY, ' .
      ' n ' . _int_column_type($self->{'wordlist_cardinality'}) .
        ' NOT NULL' .
      ')';
    $dbh->do($stmt) or 
      croak "$ME: Can't create table $_: " . $dbh->errstr;

    # create wordlist table
    $stmt = "CREATE TABLE ${idx}_wordlist " . 
      '(id ' . _int_column_type($self->{'wordlist_cardinality'}) . 
        ' AUTO_INCREMENT PRIMARY KEY, ' .
      " word VARCHAR($self->{'max_word_length'})" .
        ' BINARY NOT NULL, ' .
      "UNIQUE (word) " .
      ')';
    $dbh->do($stmt) or 
      croak "$ME: Can't create table $_: " . $dbh->errstr;

    # create stoplist table
    $stmt = "CREATE TABLE ${idx}_stoplist " .
      '(id ' . _int_column_type($self->{'wordlist_cardinality'}) . 
        ' AUTO_INCREMENT PRIMARY KEY, ' .
      " word VARCHAR($self->{'max_word_length'})" .
        ' BINARY NOT NULL, ' .
      "UNIQUE (word) " .
      ')';
    $dbh->do($stmt) or 
      croak "$ME: Can't create table $_: " . $dbh->errstr;

    # create vectorlist table
    $stmt = "CREATE TABLE ${idx}_vectorlist " .
      '(wid ' . _int_column_type($self->{'wordlist_cardinality'}) . 
        ' NOT NULL, ' .
      'did ' . _int_column_type($self->{'doclist_cardinality'}) . 
        ' NOT NULL, ' .
      'UNIQUE (wid,did), ' .
      'f FLOAT(10,4) NOT NULL' .
      ')';
    $dbh->do($stmt) or
      croak "$ME: Can't create table $_: " . $dbh->errstr;
    
    $self->{'stoplist'} = {};
      
    1;
}

sub _int_column_type {
    my $cardinality = shift;

    return 'INT UNSIGNED'       if ($cardinality >= 16*1024*1024);
    return 'MEDIUMINT UNSIGNED' if ($cardinality >= 64*1024);
    return 'SMALLINT UNSIGNED'  if ($cardinality >= 256);
    return 'TINYINT UNSIGNED';
}

sub _index_tables {
    my $self = shift;
    my $idx  = $self->{'index_name'};

    map {"${idx}_$_"} qw(doclist wordlist vectorlist stoplist);
}

sub _index_exists {
    my $self = shift;
    my $dbh  = $self->{'dbh'};
    my $idx  = $self->{'index_name'};
    my $sth;

	local $dbh->{'RaiseError'} = 0 if $dbh->{'RaiseError'};

	my %existing_tables = map { $_ => 1 } $dbh->tables;

	for ($self->_index_tables) {
		return 0 if !exists $existing_tables{$_};
	}

    1;
}

1;

__END__

=head1 NAME 

DBIx::KwIndex - create and maintain keyword indices in DBI tables

=head1 SYNOPSIS

 package MyKwIndex;
 use DBIx::KwIndex;

 sub document_sub { ... }

 package main;
 $kw = DBIx::KwIndex->new({dbh => $dbh, index_name => 'myindex'})
   or die "can't create index";

 $kw->add_document   ([1,2,3,...]) or die $kw->{ERROR};
 $kw->remove_document([1,2,3,...]) or die $kw->{ERROR};
 $kw->update_document([1,2,3,...]) or die $kw->{ERROR};

 $docs = $kw->search({ words=>'upset stomach' });
 $docs = $kw->search({ words=>'upset stomach', boolean=>'AND' });
 $docs = $kw->search({ words=>'upset stomach', start=>11, num=>10 });
 $docs = $kw->search({ words=>['upset','(bite|stomach)'], re=>1 });

 $kw->add_stop_word(['the','an','am','is','are']) or die $kw->{ERROR};
 $words = $kw->common_word(85);
 $kw->remove_word(['gingko', 'bibola']) or die $kw->{ERROR};

 $ndocs  = $kw->document_count();
 $nwords = $kw->word_count();
 $ndocs  = $kw->match_count({ words=>'upset stomach', boolean=>'OR' });

 $kw->remove_index or die $kw->{ERROR};
 $kw->empty_index  or die $kw->{ERROR};

=head1 DESCRIPTION

DBIx::KwIndex is a keyword indexer. It indexes documents and stores the
index data in database tables. You can tell DBIx::KwIndex to index [lots]
of documents and later on show you which ones contain a certain word.
The typical application of DBIx::KwIndex is in a search engine.

How to use this module:

=over 4

=item 1. Provide a database handle.

 use DBI;
 my $dbh = DBI->connect(...) or die $DBI::errstr;

=item 2. Subclass DBIx::KwIndex and provide a `document_sub' method to
retrieve documents referred by an integer id. The method should accept a
list of document ids in an array reference and return the documents in an
array reference. In this way, you can index any kind of documents that you
want: text files, HTML files, BLOB columns, etc., as long as you provide
the suitable document_sub() to retrieve the documents. The one thing to
remember is that the documents must be referred by unique integer number.
Below is a sample of a document_sub() that retrieves document from the
'content' field of a database table.

 package MyKwIndex;
 require DBIx::KwIndex;
 use base 'DBIx::KwIndex';

 sub document_sub {
    my ($self, $ary_ref) = @_;
	my $dbh = $self->{dbh};

    my $result = $dbh->selectall_arrayref(
    'SELECT id,content FROM documents
     WHERE id IN ('. join(',',@$ary_ref). ')');
    
    # if retrieval fails, you should return undef
    defined($result) or return undef;
    
    # now returns the content field in the order of the id's
    # requested. remember to return the documents exactly 
    # in the order requested!
    my %tmp = map { $_->[0] => $_->[1] } @$result;
    return [ @tmp{ @$ary_ref } ];
 }

=item 3. Create the indexer object.

 my $kw = MyKwIndex->new({
          dbh => $dbh,
          index_name => 'article_index',
          # other options...
          });

B<dbh> is the database handle. B<index_name> is the name of the index,
DBIx::KwIndex will create several tables which are all prefixed with the
index_name. The default index_name is 'kwindex'. Other options include:
B<max_word_length> (default 32).

=item 4. Index some documents. You can index one document at a time, e.g.

 $kw->add_document([1]) or die $kw->{ERROR};
 $kw->add_document([2]) or die $kw->{ERROR};

or small batches of documents at a time:

 $kw->add_document([1..10])  or die $kw->{ERROR};
 $kw->add_document([11..20]) or die $kw->{ERROR};

or large batches of documents at a time:

 $kw->add_document([1..300])   or die $kw->{ERROR};
 $kw->add_document([301..600]) or die $kw->{ERROR};

Which one to choose is a matter of memory-speed trade-off. Larger batches
will increase the speed of indexing, but with increased memory usage.

Note: DBIx::KwIndex ignores single-character words, numbers, and words
longer than 'max_word_length'.

=item 5. If you want to search the index, use the search() method.

 $docs = $kw->search({ words => 'upset stomach' });
 die "can't search" if !defined($docs);

The search() method will return an ARRAY ref containing the document ids
that matches the criteria. Other parameter include: B<num> => maximum number
of results to retrieve; B<start> => starting position (1 = from the
beginning); boolean => 'AND' or 'OR' (default is 'OR'); B<re> => use regular
expression, 1 or 0.

Note: B<num> and B<start> uses the C<LIMIT> clause (which is quite unique to
MySQL). B<re> uses the C<REGEXP> clause. Do not use these options if your
database server does not support them.

Also note: Searching is entirely done from the index. No documents will be
retrieved while searching. A simple 'relevancy' ranking is used. Search is
case-insensitive and there is no phrase-search support yet.

Some examples:

 # retrieve only the 11th-20th result.
 $docs = $kw->search({ words=>'upset stomach', start=>11, num=>10 });
 die "can't search" if !defined($docs);

 # find documents which contains all the words.
 $docs = $kw->search({ words=>'upset stomach', boolean=>'AND' });
 die "can't search" if !defined($docs);

If you just want to know how many documents match your query, use the
match_count() method. If you want to retrieve all the matches anyway, to
know how many documents match just find out the size of the match array.
That will save you from extra index access.

 # find the number of documents that match the query
 $ndocs = $kw->match_count({ words=>'halitosis' });

 # find the number of matches and retrieve only the first twenty of them
 $query = { words=>'halitosis', num=>20 };
 $ndocs = $kw->match_count($query);
 $docs  = $kw->search($query);

 # search and get the matches. get the number of matches from the
 # result set itself.
 $docs  = $kw->match_count({ words=>'halitosis' });
 $ndocs = @$docs;

=item 6. Now suppose some documents change, and you need to update the
index to reflect that. Just use the methods below.
 
 # if you want to remove documents from index 
 $kw->remove_document([90..100]) or die $kw->{ERROR};

 # if you want to update the index
 $kw->update_document([90..100]) or die $kw->{ERROR};

=back

=head1 SOME UTILITY METHODS

If you want to exclude some words (usually very common words, or "stop
words") from being indexed, do this before you index any document:

 $kw->add_stop_word(['the','an','am','is','are'])
   or die "can't add stop words";

Adding stop words is a good thing to do, as stop words are not very useful
for your index. They occur in a large proportion of documents (they do not
help searches differentiate documents) and they increase the size your index
(slowing the searches).

But which words are common in your collection? you can use the common_word
method:

 $words = $kw->common_word(85);

This will return an array reference containing all the words that occur in
at least 85% of all documents (default is 80%).

If you want to delete some words from the index:

 $kw->remove_word(['common','cold']);
   or die "can't remove words";

To get some statistics about your index:

 # the number of documents
 $ndocs = $kw->document_count();
 # the number of words
 $nwords = $kw->word_count();

Last, if you got bored with the index and want to delete it:

 $kw->remove_index or die $kw->{ERROR};

This will delete the database tables. Or, if you just want to empty
the index and start all over:

 $kw->empty_index or die $kw->{ERROR};

=head1 AUTHOR

Steven Haryanto &lt;steven@haryan.to&gt;

=head1 COPYRIGHT

Copyright (c) 2000 Steven Haryanto. All rights reserved. 

You may distribute under the terms of either the GNU General Public License
or the Artistic License, as specified in the Perl README file.

=head1 BUGS/CAVEATS/TODOS

Enable the module to use other database server (besides MySQL).
MySQL-specific SQL bits that need to be adjusted include, but not limited
to: LIMIT clause, LOCK/UNLOCK TABLES statements, REPLACE INTO/INSERT IGNORE
statements, COUNT(DISTINCT ...) group function, AUTO_INCREMENT and
INT/UNSIGNED. (Don't you just hate SQL? C<:-)> Thanks to Edwin Pratomo for
pointing these out). Currently I do not need this feature, since I only use
MySQL for current projects. Any volunteer?

Use a more correct search sorting (the current one is kinda bogus :).

Probably implement phrase-searching (but this will require a larger
vectorlist).

Probably, maybe, implement English/Indonesian stemming.

Any safer, non database-specific way to test existence of tables other than
$dbh->tables?

=head1 NOTES

At least three other Perl extensions exist for creating keyword indices and
storing them in a database: DBIx::TextIndex, MyConText,
DBIx::FullTextSearch. You might want to take a look at them before you
decide which one better suits your need. Personally I develop DBIx::KwIndex
because I want to have a module that: a) is simple and convenient to use; b)
supports updating the index without rebuilding it from scratch.

Incidentally, these three extensions and DBIx::KwIndex itself use MySQL
specifically. One that could use Interbase or Postgres would perhaps be
nice.

Advices/comments/patches welcome.

=head1 HISTORY

0001xx=first draft,satunet.com. 000320=words->scalar.
000412=0.01/documentation/cpan. 000902=update doc/fixes(see Changes)
