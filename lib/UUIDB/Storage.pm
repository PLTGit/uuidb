package UUIDB::Storage;

use v5.10;
use strict;
use warnings;

use Carp qw( croak );
use Moo;
use Types::Standard qw( InstanceOf );
use UUIDB::Util qw( check_args );

# TODO: POD, tests

has db => (
    is => "rw",
    isa => InstanceOf[qw( UUIDB )],
);

sub store_document ($$;%) { croak "The 'store_document' method must be overridden in descendantclasses" }
sub get_document   ($$$ ) { croak "The 'get_document' method must be overridden in descendantclasses"   }
sub exists         ($$  ) { croak "The 'exists' method must be overridden in descendantclasses"         }
sub delete         ($$;$) { croak "The 'delete' method must be overridden in descendantclasses"         }

# TODO: standardize_key ?

sub standardize_key ($$) {
    my ($self, $key) = @_;

    # Should also check to make sure it's a UUID, etc.
    return lc $key;
}

1;
