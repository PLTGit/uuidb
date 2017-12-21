package UUIDB::Storage;

use v5.10;
use strict;
use warnings;

use Carp            qw( croak );
use Moo;
use namespace::autoclean;
use Scalar::Util    qw( blessed             );
use Types::Standard qw( Bool InstanceOf Str );
use UUID::Tiny      qw( is_uuid_string      );
use UUIDB::Util     qw( check_args          );

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

sub set_options {
    my ($self, %opts) = @_;
    # TODO: storage_options
}

# NOTE: UUIDB.pm and UUIDB::Storage::Fileplex have some opinions as to what the
# invocation signature looks like for these.
sub delete_document { croak "'delete_document' method must be overridden in descendant classes" }
sub document_exists { croak "'document_exists' method must be overridden in descendant classes" }
sub get_document    { croak "'get_document' method must be overridden in descendant classes"    }
sub store_document  { croak "'store_document' method must be overridden in descendant classes"  }

# Simple aliases
sub delete         { shift->delete_document( @_ ) }
sub exists         { shift->document_exists( @_ ) }
sub get            { shift->get_document(    @_ ) }
sub remove         { shift->delete_document( @_ ) }
sub store          { shift->store_document(  @_ ) }

sub standardize_key {
    my ($self, $key) = @_;

    # Should also check to make sure it's a UUID, etc.
    croak "Invalid UUID"
        unless defined( $key )
        and    is_uuid_string( $key );

    return lc $key;
}

sub type {
    my ($self) = @_;
    my $type = blessed $self;
    $type =~ s/^UUIDB::Storage:://;
    return $type;
}

1;
