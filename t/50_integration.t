#!/usr/bin/env perl

use v5.10;
use strict;
use warnings;
use Test::Most;

run_tests();
done_testing();

### Define tests to be run

sub run_tests {
    test_integration();
}

# Complete run-through with a sample database
sub test_integration {
    use_ok( "UUIDB" );

    my $uuidb = UUIDB->new(
        name          => "Test",
        document_type => "JSON",
        storage_type  => "Memory",
    );

    isa_ok( $uuidb, "UUIDB" );

    my %data = (
        any => "kind",
        of  => "JSON",
        serializable => [qw(
            data
        )],
    );

    # Create it
    my $key = $uuidb->create( \%data );
    is( $uuidb->exists( $key ), 1, "Data exists in storage" );

    # Read it
    my %data_copy = %{ $uuidb->get( $key ) };
    is_deeply(
        \%data,
        \%data_copy,
        "Data successfully stored and retrieved"
    );

    # Update it
    $data{change} = "something";
    $uuidb->set( $key, \%data );

    is( $data_copy{change}, undef,
        "Copied data (non-ref) does not yet have the new value." );

    # Refresh our read
    %data_copy = %{ $uuidb->get( $key ) };
    is( $data_copy{change}, "something", "Successfully updated entry" );

    # Delete it
    is( $uuidb->delete( $key ), 1, "Deleted entry" );

    # Verify deletion
    is( $uuidb->exists( $key ), 0, "Entry no longer exists" );
    is( $uuidb->get( $key ), undef, "Non-existent key fetch returns undef" );

    # Slightly more OO version
    # Create it (which also adds it to the store)
    my $document = $uuidb->create_document( \%data );
    isa_ok( $document, "UUIDB::Document" );

    my $new_key  = $document->uuid();
    ok( $new_key ne $key, "New key issued" );

    $document->data->{still} = "simple data";
    $document->save(); # or $document->update();

    my $document_copy = $uuidb->get_document( $new_key );
    isnt( $document_copy, $document, "New document instance returned" );
    is_deeply( $document_copy->data, $document->data, "Everything matches." );
};
