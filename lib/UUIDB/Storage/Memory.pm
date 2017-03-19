package UUIDB::Storage::Memory;

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use Types::Standard qw( Any InstanceOf Map Str );
use UUIDB::Util qw( check_args );
extends qw( UUIDB::Storage );

has store => (
    is      => 'rw',
    isa     => Map[Str, Any],
    default => sub { {} },
);

sub store_document ($$;%) {
    my ($self, $document, %options) = @_;

    check_args(
        args => {
            document => $document,
        },
        must => {
            document => InstanceOf[qw( UUIDB::Document )],
        }
    );

    $document->uuid( $self->db->uuid() ) unless $document->uuid();
    my $key = $self->standardize_key( $document->uuid() );
    $self->store->{ $key } = $document->frozen();

    return $document;
}

sub get_document ($$$) {
    my ($self, $key, $document_handler) = @_;
    my $found;
    $key = $self->standardize_key( $key );
    if ( $self->exists( $key ) ) {
        $found = $document_handler->new_from_data(
            $document_handler->thaw( $self->store->{ $key } )
        );
        $found->uuid( $key );
    }
    return $found;
}

sub exists ($$) {
    my ($self, $key) = @_;
    check_args(
        args => {
            key => $key,
        },
        must => {
            key => Str,
        }
    );
    $key = $self->standardize_key( $key );
    return exists $self->store->{ $key };
}

sub delete ($$;$) {
    my ($self, $key, $warnings) = @_;
    $key = $self->standardize_key( $key );
    if ( $self->exists( $key ) ) {
        delete $self->store->{ $key };
    } elsif ( $warnings ) {
        carp "Document $key not found, nothing deleted";
    }
}

sub standardize_key ($$) {
    my ($self, $key) = @_;
    return $self->SUPER::standardize_key( $key );
}

1;
