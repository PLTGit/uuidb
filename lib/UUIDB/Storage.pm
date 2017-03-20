package UUIDB::Storage;

use v5.10;
use strict;
use warnings;

use Carp qw( croak );
use Moo;
use Types::Standard qw( InstanceOf );
use UUID::Tiny qw( is_uuid_string );
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

# Simple aliases
sub store          ($$;%) { &store_document }
sub get            ($$$ ) { &get_document   }

# TODO: standardize_key ?

sub standardize_key ($$) {
    my ($self, $key) = @_;

    # Should also check to make sure it's a UUID, etc.
    croak "Invalid UUID"
        unless defined( $key )
        and    is_uuid_string( $key );

    return lc $key;
}

1;