package UUIDB::Document::YAML;

use v5.10;
use strict;
use warnings;

use YAML::XS qw( Dump Load );
use namespace::autoclean;

use Moo;

# TODO: POD, tests
extends qw( UUIDB::Document );

sub type { "YAML" }

sub freeze {
    my ($self, $data) = @_;
    return Dump( $data );
}

sub thaw {
    my ($self, $frozen) = @_;
    return Load( $frozen );
}

1;
