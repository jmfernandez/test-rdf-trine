#!/usr/bin/perl

use warnings 'all';
use strict;

use File::Spec qw();
use FindBin;

# We cannot use local::lib because at this point we cannot be sure
# about having it installed
use lib File::Spec->catdir($FindBin::Bin,'deps','lib','perl5');

use Digest::MD5 qw(md5_hex);

use RDF::Trine;
use RDF::Trine::Parser;
use RDF::Trine::Store::DBI;

use RDF::Query;
use RDF::Query::Parser;
use Time::HiRes qw(time);

sub timeMe(&);

sub timeMe(&) {
        my($code) = @_;

        my $bef = time;
        eval {     
                $code->();
        };
        my $dif = time - $bef;

        return $dif;
}


sub getModel($$) {
	my($dbh,$modelurl) = @_;
	
	# The name of the model is the URL itself
	my $modelname = md5_hex($modelurl);
	
	# Second, create a new Store object with the database connection
	# and specifying (by name) which model in the Store you want to use
	my $store = RDF::Trine::Store::DBI->new( $modelname, $dbh );
	
	my $doLoad = $store->size() == 0;
	
	# Finally, wrap the Store objec into a Model, and use it to access your data
	my $model = RDF::Trine::Model->new($store);
	
	if($doLoad) {
		$model->begin_bulk_ops();
		RDF::Trine::Parser->parse_url_into_model($modelurl, $model);
		$model->end_bulk_ops();
	}
	$store->init();
	# exit(1);
	
	return $model;
}



#my $sparql_query = <<'SPARQL' ;
#PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#PREFIX owl: <http://www.w3.org/2002/07/owl#>
#SELECT ?x WHERE {
#?x rdfs:label ?q .
#{ ?x rdf:type owl:Class }
#UNION
#{ ?x rdf:type rdfs:Class } .
#}
#SPARQL


my $dbfile = './trine.sqlite3';
my $dsn = "dbi:SQLite:dbname=$dbfile";
my $dbh = DBI->connect($dsn,"","", {AutoCommit => 0});

if($dbh) {
	my $model = undef;
	my $modelurl = undef;
	my $q = undef;
	my $sparql_query = undef;
	my $query = undef;
	my $iterator = undef;
	
	print "First\n";
	$modelurl = 'https://www.ebi.ac.uk/efo/releases/v2018-02-15/efo.owl';
	$model = getModel($dbh,$modelurl);
	print "* $modelurl has " . $model->size . " RDF statements\n";
	
	# SPARQL SELECT Query
	print "\tSearch by IRI\n";
	$q = "http://www.ebi.ac.uk/efo/EFO_0003042";
	$sparql_query = <<'SPARQL' ;
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT (?q AS ?res) WHERE {
{ ?q a owl:Class }
UNION
{ ?q a rdfs:Class }
}
SPARQL
	
	my $first_iriTime = timeMe {
		$query = RDF::Query->new($sparql_query);
		unless(defined($query)) {
			print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
		}
		$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_uri($q) } );
		if(defined($iterator)) {
			while (my $row = $iterator->next) {
				# $row is a HASHref containing variable name -> RDF Term bindings
				print "GUAY\n";
				print "\t\t",$row->{ 'res' }->as_string,"\n";
			}
		} else {
			print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
		}
	};
	print "\tTime: $first_iriTime\n";
	
	# SPARQL SELECT Query
	print "\tSearch by IRI suffix\n";
	$q = "EFO_0003042";
	$sparql_query = <<'SPARQL' ;
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT (?x AS ?res) WHERE {
{ ?x a owl:Class }
UNION
{ ?x a rdfs:Class }
FILTER strends(str(?x),?q) .
}
SPARQL
	
	my $first_iriSuffixTime = timeMe {
		$query = RDF::Query->new($sparql_query);
		unless(defined($query)) {
			print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
		}
		$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_literal($q,undef,'http://www.w3.org/2001/XMLSchema#string') } );
		if(defined($iterator)) {
			while (my $row = $iterator->next) {
				# $row is a HASHref containing variable name -> RDF Term bindings
				print "GUAY\n";
				print "\t\t",$row->{ 'res' }->as_string,"\n";
			}
		} else {
			print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
		}
	};
	print "\tTime: $first_iriSuffixTime\n";
	
	
	# SPARQL SELECT Query
	print "\tSearch by IRI suffix (no result)\n";
	$q = "EFO_000304";
	
	#$query = RDF::Query->new($sparql_query);
	#unless(defined($query)) {
	#	print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
	#}
	my $first_iriSuffixNoTime = timeMe {
		$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_literal($q,undef,'http://www.w3.org/2001/XMLSchema#string') } );
		if(defined($iterator)) {
			while (my $row = $iterator->next) {
				# $row is a HASHref containing variable name -> RDF Term bindings
				print "GUAY\n";
				print "\t\t",$row->{ 'res' }->as_string,"\n";
			}
		} else {
			print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
		}
	};
	print "\tTime: $first_iriSuffixNoTime\n";
	
	print "Second\n";
	$modelurl = 'http://purl.obolibrary.org/obo/cl/releases/2018-07-07/cl.owl';
	$model = getModel($dbh,$modelurl);
	print "* $modelurl has " . $model->size . " RDF statements\n";
	
	print "Third\n";
	$modelurl = 'http://purl.obolibrary.org/obo/uberon/releases/2018-07-30/uberon.owl';
	$model = getModel($dbh,$modelurl);
	print "* $modelurl has " . $model->size . " RDF statements\n";
	
	print "Fourth\n";
	$modelurl = 'http://purl.obolibrary.org/obo/obi/2018-08-27/obi.owl';
	$model = getModel($dbh,$modelurl);
	print "* $modelurl has " . $model->size . " RDF statements\n";

	# SPARQL SELECT Query
	print "\tSearch by label\n";
	my $labelRes = undef;
	$q = "ChIP-seq assay";
	$sparql_query = <<'SPARQL' ;
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT (?x AS ?res) WHERE {
{ ?x a owl:Class }
UNION
{ ?x a rdfs:Class } .
?x rdfs:label ?q .
}
SPARQL
	
	my $fourth_labelTime = timeMe {
		$query = RDF::Query->new($sparql_query);
		unless(defined($query)) {
			print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
		}
		$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_literal($q,undef,'http://www.w3.org/2001/XMLSchema#string') } );
		if(defined($iterator)) {
			while (my $row = $iterator->next) {
				# $row is a HASHref containing variable name -> RDF Term bindings
				print "GUAY\n";
				$labelRes = $row->{ 'res' };
				print "\t\t",$labelRes->as_string,"\n";
			}
		} else {
			print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
		}
	};
	print "\tTime: $fourth_labelTime\n";

	# SPARQL SELECT Query
#	print "\tCheck ancestor by IRI\n";
#	$q = "http://purl.obolibrary.org/obo/OBI_0000716";
#	$sparql_query = <<'SPARQL' ;
#PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#PREFIX owl: <http://www.w3.org/2002/07/owl#>
#SELECT ?res WHERE {
#{ ?x a owl:Class }
#UNION
#{ ?x a rdfs:Class } .
#{ ?res a owl:Class }
#UNION
#{ ?res a rdfs:Class } .
#?q rdfs:subClassOf* ?res .
#}
#SPARQL
#	
#	$query = RDF::Query->new($sparql_query);
#	unless(defined($query)) {
#		print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
#	}
#	$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_uri($q), 'res' => RDF::Query::Parser->new_uri('http://purl.obolibrary.org/obo/BFO_0000001') } );
#	if(defined($iterator)) {
#		while (my $row = $iterator->next) {
#			# $row is a HASHref containing variable name -> RDF Term bindings
#			print "GUAY\n";
#			print "\t\t",$row->{ 'res' }->as_string,"\n";
#		}
#	} else {
#		print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
#	}

	# SPARQL SELECT Query
	print "\tObtain ancestors from label result (iterative)\n";
	$sparql_query = <<'SPARQL' ;
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?res WHERE {
	{ ?q a owl:Class }
	UNION
	{ ?q a rdfs:Class } .
	?q rdfs:subClassOf ?res .
}
SPARQL
	
	my $fourth_iriAncestorsIterativeTime = timeMe {
		$query = RDF::Query->new($sparql_query);
		unless(defined($query)) {
			print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
		}
		my @iterQ = ( $labelRes );
		my @iterRes = ();
		foreach my $iq (@iterQ) {
			$iterator = $query->execute( $model,'bind' => { 'q' => $iq } );
			if(defined($iterator)) {
				while (my $row = $iterator->next) {
					# $row is a HASHref containing variable name -> RDF Term bindings
					my $iterRes = $row->{ 'res' };
					push(@iterQ,$iterRes);
					push(@iterRes,$iterRes);
				}
			} else {
				print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
			}
		}
		
		if(scalar(@iterRes) > 0) {
			print "GUAY\n";
			print "\t\t", join(" ",map { $_->as_string() } @iterRes),"\n";
		}
	};
	print "\tTime: $fourth_iriAncestorsIterativeTime\n";

	# SPARQL SELECT Query
	#$dbh->trace($dbh->parse_trace_flags('SQL|1|test'));
	print "\tObtain ancestors by label (Slooooooooooooooooooooooow)\n";
	$q = "ChIP-seq assay";
	$sparql_query = <<'SPARQL' ;
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?res WHERE {
	?x rdfs:subClassOf* ?res .
	?x rdfs:label ?q .
	{ ?x a owl:Class }
	UNION
	{ ?x a rdfs:Class } .
}
SPARQL
	
#{
#?x rdfs:subClassOf* ?res .
#?x a owl:Class .
#?x rdfs:label ?q .
#}
#UNION
#{
#?x rdfs:subClassOf* ?res .
#?x a rdfs:Class .
#?x rdfs:label ?q .
#}

	my $fourth_iriAncestorsNativeTime = timeMe {
		$query = RDF::Query->new($sparql_query);
		unless(defined($query)) {
			print STDERR "QUERY PARSE ERROR: NOOOOOOOOOs",RDF::Query->error(),"\n";
		}
		$iterator = $query->execute( $model,'bind' => { 'q' => RDF::Query::Parser->new_literal($q,undef,'http://www.w3.org/2001/XMLSchema#string') } );
		if(defined($iterator)) {
			while (my $row = $iterator->next) {
				# $row is a HASHref containing variable name -> RDF Term bindings
				print "GUAY\n";
				print "\t\t",$row->{ 'res' }->as_string,"\n";
			}
		} else {
			print STDERR "QUERY RUN ERROR: ",$query->error(),"\n";
		}
	};
	print "\tTime: $fourth_iriAncestorsNativeTime\n";
	
	
	# At last, disconnect from the database
	$dbh->disconnect();
}