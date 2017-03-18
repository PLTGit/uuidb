#!/usr/bin/env perl

use v5.10;
use strict;
use warnings;
use Test::Most;
use Data::Dump qw( dd pp );

run_tests();
done_testing();

###

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

    my $key = $uuidb->create( \%data );

    my %data_copy = %{ $uuidb->get( $key ) };
    is_deeply(
        \%data,
        \%data_copy,
        "Data successfully stored and retreived"
    );

    note "YOU ARE HERE: Keep working through the synopsis example";
};
