package UUIDB::Document::Raw;

use v5.10;
use strict;
use warnings;

use Moo;

# Does absolutely no translation whatsoever.
# TODO: maybe provide the option for deep cloning.

# TODO: POD, tests
extends qw( UUIDB::Document );

sub type { "Raw" }

sub freeze ($;$) {
    my ($self, $data) = @_;
    return $data;
}

sub thaw ($$) {
    my ($self, $frozen) = @_;
    return $frozen;
}

1;
