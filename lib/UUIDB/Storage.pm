package UUIDB::Storage;

use v5.10;
use strict;
use warnings;

use Carp qw( croak );
use Moo;
use Types::Standard qw( Bool InstanceOf Str );
use UUID::Tiny qw( is_uuid_string );
use UUIDB::Util qw( check_args );

# TODO: POD, tests

has db => (
    is  => "rw",
    isa => InstanceOf[qw( UUIDB )],
);

has readonly => (
    is      => "rw",
    isa     => Bool,
    default => sub { 0 },
);

# TODO: unit test for this.
before [qw( store_document delete )] => sub {
    my ($self) = @_;
    croak "Cannot modify storage when readonly" if $self->readonly;
};

sub BUILD {
    my ($self, $opts) = @_;
    # Remove any of those settings which are attribute specific.
    # Pass the remainder onto options.
    $self->set_options( %$opts );
}

sub set_options {
    my ($self, %opts) = @_;
    # TODO: storage_options
}

sub store_document { croak "The 'store_document' method must be overridden in descendantclasses" }
sub get_document   { croak "The 'get_document' method must be overridden in descendantclasses"   }
sub exists         { croak "The 'exists' method must be overridden in descendantclasses"         }
sub delete         { croak "The 'delete' method must be overridden in descendantclasses"         }

# Simple aliases
sub store          { &store_document }
sub get            { &get_document   }

# TODO: standardize_key ?

sub standardize_key {
    my ($self, $key) = @_;

    # Should also check to make sure it's a UUID, etc.
    croak "Invalid UUID"
        unless defined( $key )
        and    is_uuid_string( $key );

    return lc $key;
}

1;
