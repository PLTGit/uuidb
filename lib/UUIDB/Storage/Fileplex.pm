package UUIDB::Storage::Fileplex;

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use Digest::MD5     qw( md5_hex            );
use File::Path      qw( mkpath             );
use Types::Standard qw( Bool Maybe Ref Str );
use UUIDB::Util     qw( check_args         );

extends qw( UUIDB::Storage );

# TODO: POD

has path => (
    is => "rw",
    isa => Str,
);

# Can be used to introduce higher local variability in those sequences
# used for managing pathing.
#
# It is *highly* recommended to use the "propogate_uuid" setting in
# UUIDB::Document options when this is enabled, since otherwise the reverse
# association from hashed key to UUID is effectively impossible to determine.
has rehash_key => (
    is      => 'rw',
    isa     => Maybe[Bool, Ref[qw( CODE )]],
    default => sub { 0 },
);

sub BUILD {
    my ($self, %opts) = @_;
    # Remove any of those settings which are attribute specific.
    # Pass the remainder onto options.
    $self->set_options( %opts );
}

sub set_options ($%) {
    my ($self, %opts) = @_;
    # TODO: storage_options
    # path (location of the directory in which to build)
    # indexes => {
    #    field_name => sub { qw( which knows how to get the "name" value from a Document }
    # }
}

sub store_document ($$;%) {
}

sub get_document ($$$) {
}

sub exists ($$) {
}

sub delete ($$;$) {
}

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

sub mkdir ($$) {
}

sub init_check ($) {
    my ($self) = @_;
    croak "Path not set" unless    $self->path;
    croak "Invalid path" unless -d $self->path;
    carp "Path not writable"
        unless -d $self->path
        and    -w $self->path
        and      !$self->readonly;
}

1;
