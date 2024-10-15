#!/usr/bin/env perl

use v5.10;
use strict;
use warnings;
use Data::Dump qw( dd );

use UUIDB;

my $uuidb = UUIDB->new(

    document_type    => "JSON",
    document_options => {
        propagate_uuid => 1,
    },

    storage_type    => "Fileplex",
    storage_options => {
        path  => "/home/ptom/.uuidb/taxonomi",
        index => [qw(
            only_one
            name
        )],
        overwrite_newer => 1,
        rehash_key      => 1,
    },

);

my $data = {
    name => "Stu is a disco fiend",
    this => "that",
    only_one => "I am the only one by this name",
};

my $key;
$key = $uuidb->create( $data ); # for 0 .. 1000;
say "Created $key";
my $document = $uuidb->get_document( $key );
# dd $document;
$document->meta->{ctime} -= 5;
$document->data->{these} = "those";
$document->save;

say "Getting document copy";
my $doc2 = $uuidb->get_document( $key );
say "UUID: " . ( $doc2->uuid_from_data // 'undef' );
my ($path) = $uuidb->exists( $key );
say "Path: $path";

say "Searching index for 'Stu '...";
dd [ $uuidb->storage->search_index( name => "Stu " ) ];
say "Searching documents for 'Stu is a disco fiend'";
my @entries = $uuidb->storage->search( name => "Stu is a disco fiend", 1 );
dd \@entries;
say "Searching documents for 'Stu is '";
@entries = $uuidb->storage->search( name => "Stu is " );
dd \@entries;

say "Removing $key...";
$uuidb->delete( $key );

# dd $uuidb->storage;

# dd $key;
# dd $data;

# NOW: see if we can find it in the index, and read it out.
# dd $_ for $uuidb->storage->search_index( name => "Stuff to be indexed", 1 );
# dd $_ for $uuidb->storage->search_index( name => "Stuff to be indexed", 1, 1 );

__DATA__

foreach my $entry ( @entries ) {
    dd $uuidb->get( $entry );
}
