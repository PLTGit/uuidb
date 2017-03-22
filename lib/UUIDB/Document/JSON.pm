package UUIDB::Document::JSON;

use v5.10;
use strict;
use warnings;

use Moo;
use JSON::PP qw( decode_json encode_json );

# TODO: POD, tests
extends qw( UUIDB::Document );

sub type { "JSON" }

sub freeze {
    my ($self, $data) = @_;
    return encode_json( $data );
}

sub thaw {
    my ($self, $frozen) = @_;
    return decode_json( $frozen );
}

1;
