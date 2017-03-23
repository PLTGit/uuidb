package UUIDB::Storage::Fileplex;

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use Digest::MD5     qw( md5_hex            );
use File::Path      qw( mkpath             );
use Types::Standard qw( ArrayRef Bool InstanceOf Maybe Ref Str );
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

# Should this be an array or a hash?  Or a map, of some kind?
has index => (
    is      => 'rw',
    isa     => ArrayRef[Str],
    default => sub { [] },
);

sub set_options {
    my ($self, %opts) = @_;
    # TODO: storage_options
    # path (location of the directory in which to build)
    # indexes => {
    #    field_name => sub { qw( which knows how to get the "name" value from a Document }
    # }
}

sub store_document {
    my ($self, $document) = @_;

    check_args(
        args => { document => $document                         },
        must => { document => InstanceOf[qw( UUIDB::Document )] },
    );

    $self->init_check();
    $self->init_store();

    my $data = $document->frozen;
    # TODO: if $data is not defined, delete an existing record by the same key
    # (since undef and deleted are equivalent).  If it does not exist, simply
    # return undef, since there's nothing to store.

    my ($document_path, $filename) = $self->make_document_path( $document );
    my $storage_path = join( "/", $self->data_path, $document_path );
    $self->mkdir( $storage_path ) unless -d $storage_path;

    open( my $fh, '>', "$storage_path/$filename" );
    print $fh $data;
    close( $fh );

    # TODO: error checking on the above

    # ALSO TODO: process indexes.  In fact, process those in advance, and do all
    # the file flushing at once.
    return $document->uuid();
}

sub get_document {
}

sub exists {
}

sub delete {
}

sub standardize_key {
    my ($self, $key) = @_;
    # This also does an is_uuid_string check for us.
    $key = $self->SUPER::standardize_key( $key );
    if ( my $rehash = $self->rehash_key ) {
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

sub rehash_algorithm {
    my ($self, $key) = @_;
    return md5_hex( $key );
}

sub make_document_path {
    my ($self, $document) = @_;

    $self->init_check();

    croak "Document lacks UUID" unless $document->uuid();
    my $key = $self->standardize_key(  $document->uuid() );

    croak "No key" unless length( $key );

    # WARNING: magic numbers
    # TODO: make this variable?
    my $prefix_breakdown_length = 2 * 3;
    unless ( length( $key ) >= $prefix_breakdown_length ) {
        carp "Insufficient key length, padding";
        $key .= ( "0" x ( $prefix_breakdown_length - length( $key ) ) );
    }

    my $path = join(
        "/",
        substr( $key, 0, 2 ),
        substr( $key, 2, 2 ),
        substr( $key, 4, 2 ),
    );

    my $file = $key;
    if ( my $suffix = $document->suffix ) {
        $file.= ".$suffix";
    }
    my @parts = ( $path, $file );

    return ( wantarray ? @parts : join( "/", @parts ) );
}

sub mkdir {
    my ($self, @paths) = @_;
    return mkpath( @paths );
}

sub init_check {
    my ($self) = @_;
    croak "Path not set" unless    $self->path;
    croak "Invalid path (not found)" unless -d $self->path;
    croak "Path not writable"
        unless -d $self->path
        and    -w $self->path
        and      !$self->readonly;
}

sub init_store {
    my ($self) = @_;
    $self->init_check();

    $self->mkdir( grep { ! -d } (
        $self->data_path,
        $self->index_path,
    ) );
}

sub data_path {
    my ($self) = @_;
    my $base_path = $self->path;
    croak "No base path set" unless $base_path;
    return "$base_path/data";
}

sub index_path {
    my ($self)    = @_;
    my $base_path = $self->path;
    croak "No base path set" unless $base_path;
    return "$base_path/index";
}

# Find an index entry
sub search_index {
    my ($self, $index, $starts_with) = @_;
}

# Given an index entry, return the list of UUIDs it contains.
sub lookup_by_index {
    my ($self, $index, $value) = @_;
}

1;
