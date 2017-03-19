package UUIDB::Storage::Fileplex;

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use UUIDB::Util qw( check_args );
use Types::Standard qw( Bool Ref );
use Digest::MD5 qw( md5_hex );
extends qw( UUIDB::Storage );

# TODO: POD

# Can be used to introduce higher local variability in those sequences
# used for managing pathing.
#
has rehash_key => (
    is      => 'rw',
    isa     => Maybe[Bool, Ref[qw( CODE )]],
    default => sub { 0 },
);

has rehash_algorithm => (
    is      => 'rw',
    isa     => Ref[qw( CODE )],
    default => sub { \%md5_hex },
);

sub store_document ($$;%) {
}

sub get_document ($$$) {
}

sub exists ($$) {
}

sub delete ($$;$) {
}

# TODO: storage_options
# path (location of the directory in which to build)
# indexes => {
#    field_name => sub { qw( which knows how to get the "name" value from a Document }
# }

sub standardize_key ($$) {
    my ($self, $key) = @_;
    # This also does an is_uuid_string check for us.
    $key = $self->SUPER::standardize_key( $key );
    if ( my $rehash = $self->rehash ) {
        if ( ref $rehash ) {
            # Run the coderef instead
            $key = $rehash->( $key );
        } else {
            # TODO: definable hash engine to use.
            $key = $self->rehash_algorithm( $key );
        }
    }
    return $key;
}

sub rehash_algorithm ($$) {
    my ($self, $key) = @_;
    return md5_hex( $key );
}

# Assumes the document stores data internally as a hash, and is defined by the
# time we get here.
sub simple_value_extractor ($$$) {
    my ($self, $document, $field) = @_;
    return $document->data->{ $field };
}


1;
