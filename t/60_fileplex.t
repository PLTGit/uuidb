#!/usr/bin/env perl

use v5.10;
use strict;
use warnings;
use IO::All;
use Test::Most;

run_tests();
done_testing();

sub run_tests() {
    setup();
    eval { test_fileplex() };
    fail $@ if $@;
    tear_down();
}

my $tmp_path;
sub setup {
    die "No temp directory available" unless io->tmpdir;

    $tmp_path = io->tmpdir . "/fileplex_test$$";
    die "Collision detected with temporary DB directory $tmp_path"
        if -e $tmp_path;

    note "Identified temporary directory $tmp_path";
}

sub test_fileplex {
    use_ok( "UUIDB" );

    my $uuidb;
    $uuidb = UUIDB->new(
        document_type    => "JSON",
        document_options => {
            propagate_uuid => 1,
        },

        storage_type    => "Fileplex",
        storage_options => {
            path  => $tmp_path,
            index => [qw(
                only_one
                name
            )],
            overwrite_newer => 0,
            rehash_key      => 0,
        },

    );
    isa_ok( $uuidb->storage, 'UUIDB::Storage::Fileplex' );

    my %data = (
        name     => "Stu is a disco fiend",
        this     => "that",
        only_one => "I am the only one by this name",
    );

    my %data_too = (
        name => "Stuart little",
        this => "something else",
    );

    # E.g., 5880a761-3143-4ac2-afe3-c5b4e310d29e
    my $uuid_regex = qr//; qr/\A
        [0-9a-f]{8}-        # First segment  of  8
        (?:[0-9a-f]{4}-){3} # Trhee segments of  4
        [0-9a-f]{12}        # Final segment  of 12
    \Z/x;

    my $key     = $uuidb->create( \%data     );
    my $key_too = $uuidb->create( \%data_too );
    my $key_tre = $uuidb->create( \%data_too );

    like( $key,     $uuid_regex, "Created UUID key for first data"  );
    like( $key_too, $uuid_regex, "Created UUID key for second data" );
    like( $key_tre, $uuid_regex, "Created UUID key for second data copy"  );
    isnt( $key,     $key_too,    "UUIDs for documents differ as expected" );
    isnt( $key_too, $key_tre,    "UUIDs for similar documents differ as expected" );

    my $document = $uuidb->get_document( $key );
    isa_ok( $document, 'UUIDB::Document' );
    is_deeply( $document->data, \%data, "Retrieved document data matches" );
    isnt( $document->data, \%data, "Retrieved documednt data is new instance (not ref)" );

    note "Exploring indexing operations";
    my @expansions = $uuidb->storage->search_index( name => "Stu" );
    is_deeply( \@expansions, [ $data{name}, $data_too{name} ],
        "Found 2 expansions for 3 indexed documednts (in the right order)" );

    my @found = $uuidb->storage->search( name => "Stu" );
    is_deeply(
        \@found,
        [ sort ( $key, $key_too, $key_tre )],
        "Open search on short fragments produced all 3 documents",
    );

    return 1;
    
    # TODO: exercising all the other underlying methods for key rehashing, index
    # updates, ctime modification detection and rejection, etc.
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
    note [ $uuidb->storage->search_index( name => "Stu " ) ];
    say "Searching documents for 'Stu is a disco fiend'";
    my @entries = $uuidb->storage->search( name => "Stu is a disco fiend", 1 );
    note \@entries;
    say "Searching documents for 'Stu is '";
    @entries = $uuidb->storage->search( name => "Stu is " );
    note \@entries;

    say "Removing $key...";
    $uuidb->delete( $key );

    # note $uuidb->storage;

    # note $key;
    # note $data;

    # NOW: see if we can find it in the index, and read it out.
    # note $_ for $uuidb->storage->search_index( name => "Stuff to be indexed", 1 );
    # note $_ for $uuidb->storage->search_index( name => "Stuff to be indexed", 1, 1 );

}

sub tear_down {
    note "Removing temporary directory $tmp_path";
    io->dir( $tmp_path )->rmtree;
}
