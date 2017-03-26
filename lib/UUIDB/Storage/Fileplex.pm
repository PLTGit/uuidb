package UUIDB::Storage::Fileplex;

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use Digest::MD5     qw( md5_hex    );
use File::Find      qw( find       );
use File::Path      qw( mkpath     );
use Types::Standard qw( ArrayRef Bool InstanceOf Int Maybe Ref Str );
use UUIDB::Util     qw( check_args );

extends qw( UUIDB::Storage );

# TODO: POD

has path => (
    is => "rw",
    isa => Str,
);

has data_path => (
    is      => "rw",
    isa     => Str,
    default => sub { "data" },
);

has index_path => (
    is      => "rw",
    isa     => Str,
    default => sub { "index" },
);

has plex_chunks => (
    is      => "rw",
    isa     => Int,
    default => sub { 3 },
);

has plex_chunk_length => (
    is      => "rw",
    isa     => Int,
    default => sub { 2 },
);

has index_chunks => (
    is      => "rw",
    isa     => Int,
    default => sub { 3 },
);

has index_chunk_length => (
    is      => "rw",
    isa     => Int,
    default => sub { 2 },
);

has index_suffix => (
    is      => "rw",
    isa     => Str,
    default => sub { "idx" },
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

    my ($document_path, $filename) = $self->compose_document_path(
        $document->uuid,
        $document->suffix,
        1,
    );
    $self->mkdir( $document_path ) unless -d $document_path;

    # TODO: locking, overwrite warnings (if the document has been updated more
    # recently than the local $document believes it has, or fails to match the
    # "current data" hash (which we don't actually have yet), etc., etc.
    open( my $fh, '>', "$document_path/$filename" );
    print $fh $data;
    close( $fh );

    # TODO: error checking on the above

    # ALSO TODO: process indexes.  In fact, process those in advance, and do all
    # the file flushing at once.
    my $uuid    = $document->uuid;
    my @indexes = @{ $self->index };
    if ( scalar @indexes ) {
        my $values = $document->extract( @indexes );
        DOC_INDEX: foreach my $index ( @indexes ) {
            my $value = $$values{ $index };
            next DOC_INDEX unless defined $value
                           and    length  $value;

            $self->save_index( $index, $value, $uuid );
        }
    }

    return $document->uuid();
}

sub get_document {
    my ($self, $uuid, $document_handler) = @_;
    # TODO: this
    check_args(
        args => {
            uuid             => $uuid,
            document_handler => $document_handler,
        },
        must => {
            uuid             => Str, # TODO: is_uuid_string ?
            document_handler => InstanceOf[qw( UUIDB::Document )],
        },
    );
    my $path = $self->exists( $uuid, $document_handler->suffix, 1 );
    return undef unless $path;

    open( my $fh, "<", $path ) or croak "Could not open document file for reading";
    my @data = <$fh>;
    close( $fh );

    my $document = $document_handler->new_from_data(
        $document_handler->thaw( join( "", @data ) )
    );
    $document->uuid( $uuid );

    return $document;
}

sub exists {
    my ($self, $uuid, $suffix, $as_path) = @_;
    check_args(
        args => {
            uuid   => $uuid,
            suffix => $suffix,
        },
        must => { uuid   => Str        }, # TODO: is_uuid_string ?
        can  => { suffix => Maybe[Str] },
    );
    $suffix //= $self->db->default_document_handler->suffix;
    my $path = $self->compose_document_path( $uuid, $suffix, 1 );
    return undef unless -f $path;
    return ( $as_path ? $path : 1 );
}

sub delete {
    my ($self, $uuid, $warnings) = @_;
    $uuid = $self->standardize_key( $uuid );
    if ( my $path = $self->exists( $uuid, undef, 1 ) ) {
        # TODO: clear from various indexes?
        unlink $path or croak "Could not remove document from storage";
        return 1;
    } elsif ( $warnings ) {
        carp "Document not found, nothing deleted";
    }
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

sub compose_document_path {
    my ($self, $uuid, $suffix, $full) = @_;

    $self->init_check();

    my $key = $self->standardize_key( $uuid );

    croak "No key" unless length( $key );

    my @parts = build_chunked_path(
        $key,
        $self->plex_chunks,
        $self->plex_chunk_length,
        $suffix,
    );
    if ( $full ) {
        $parts[0] = $self->storage_path(
            $self->data_path,
            $parts[0],
        );
    }
    return ( wantarray ? @parts : join( "/", @parts ) );
}

# TODO: nameclean this
sub build_chunked_path ($$$;$) {
    my (
        $key,
        $chunks,
        $chunk_length,
        $suffix,
    ) = @_;
    check_args(
        args => {
            key          => $key,
            chunks       => $chunks,
            chunk_length => $chunk_length,
            suffix       => $suffix,
        },
        must => {
            key          => [Str, qr/[^\s]/],
            chunks       => Int,
            chunk_length => Int,
        },
        can => {
            suffix => Maybe[Str],
        },
    );

    croak "Need at least one chunk"               unless $chunks       >= 1;
    croak "Chunks must have at least 1 character" unless $chunk_length >= 1;

    my $breakdown_length = $chunks * $chunk_length;

    if ( length( $key ) < $breakdown_length ) {
        carp "Insufficient key length, padding";
        $key .= ( "0" x ( $breakdown_length - length( $key ) ) );
    }

    my $path = join(
        "/",
        map { substr(
            $key,
            $_ * $chunk_length,      # Offset for chunk start
            $chunk_length            # How much data to chunk?
        ) } ( 0 .. ( $chunks - 1 ) ) # 0 based chunk no. index
    );

    my $file = $key;
    if ( defined $suffix && length( $suffix) ) {
        $file.= ( $suffix !~ m/^\./ ? "." : "" ) . $suffix;
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

    $self->mkdir( $_ ) for grep { ! -d } (
        $self->storage_path( $self->data_path  ),
        $self->storage_path( $self->index_path ),
    );
}

sub storage_path {
    my ($self, @dirs) = @_;
    my $base_path = $self->path;
    croak "No base path set" unless $base_path;
    return join( "/", $base_path, @dirs );
}

# I need to do some thinking out loud.
# There are 4 fundamental operations regarding indexes (a 5th, if we allow
# for re-indexing in bulk, but that's really only an extension of these
# others).
# save: given the name of an index, e.g., "object_name", and a value, like
# "my heroic object", store a UUIDB in association with that index.
#
# clear: remove a UUIDB from "object_name"/"my heroic object"
#
# search: find named entries in an index "object_name"/"my*" => "my heroic object"
#
# list: @uuids <= "object_name"/"my heroic object"
#
# In order to support these we need common routines for:
# - Given an index name and a value, construct a path under which that
#   index would b estored.
# - hydrate / store UUIDB lists from/to the target path
# - Create an object at that path with the list of values it contains
# - Remove the object at that path if there are no other entries.
# - Remove the path if it contains no more files.
#
# HARD PROBLEM: What do we do about changed values in the index?  Should
# they continue to appear under the old name?  Consumers of this library
# will need to be aware of the consequences.  "Always available under current
# name, may be available under prior name (until re-indexed)."
sub save_index {
    my ($self, $index, $value, $uuid) = @_;
    check_args(
        args => {
            index => $index,
            value => $value,
            uuid  => $uuid,
        },
        must => {
            index => Str,
            value => Str,
            uuid  => Str, # is_uuid_string ?
        },
    );
    # Load the existing index, if any
    # Add the new UUID to the list
    # Write the list back to the index
    my ($index_path, $filename) = $self->compose_index_path( $index, $value );

    # TODO: this logic will need to happen in multiple places, when resolving
    # the complete index path.  I have to have *too* many little subs running
    # around, so find an appropriate place to stick it.  Maybe a "full_path"
    # bool to "make_index_path".  And that should probably be "construct"
    # instead of "make", because we haven't done a mkdir yet.
    my $index_name = $self->standardize_index_name( $index );
    my $storage_path = $self->storage_path(
        $self->index_path,
        $index_name,
        $index_path,
    );
    $self->mkdir( $storage_path ) unless -d $storage_path;
    my $full_path = "$storage_path/$filename";
    my @list = $uuid;
    # TODO: replace this with "lookup by index" call
    if ( -f $full_path ) { # TODO: this is needed in lookup_by_index
        my %data = map { $_ => 1 } @list;
        open( my $fh, "<", $full_path );
        while (my $entry = <$fh>) {
            chomp $entry;
            $data{$entry} = 1;
        }
        close( $fh );
        @list = keys %data;
    }
    open( my $fh, ">", $full_path );
    # TODO: benchmarking:
    # Is it better to iterate and write, or should this be a join instead?
    print $fh "$_\n" for @list;
    close( $fh );
}

sub clear_index {
    my ($self, $index, $value, $uuid) = @_;
    # TODO: this
}

sub compose_index_path {
    my ($self, $index, $key, $full) = @_;
    check_args(
        args => {
            index => $index,
            key   => $key,
        },
        must => {
            index => [Str, qr/./],
            key   => [Str, qr/./],
        },
    );
    my @parts = build_chunked_path(
        $self->index_key( $key ),
        $self->index_chunks,
        $self->index_chunk_length,
        $self->index_suffix,
    );
    if ( $full ) {
        $parts[0] = $self->storage_path(
            $self->index_path,
            $self->standardize_index_name( $index ),
            $parts[0],
        );
    }
    return ( wantarray ? @parts : join( "/", @parts ) );
}

sub index_key {
    my ($self, $key) = @_;

    check_args(
        args => { index => $key         },
        must => { index => [Str, qr/./] },
    );

    return unpack( "H*", $key );
}

sub standardize_index_name {
    my ($self, $index_name) = @_;
    return $self->index_key( $index_name );
}

# Find an index entry
sub search_index {
    my ($self, $index, $starts_with, $exact_match, $as_file) = @_;

    # TODO: What's the minimum length required?

    if ( $exact_match ) {
        # Build the full path, return it if -f $full_path or undef otherwise.
        my ($path, $file) = $self->compose_index_path( $index, $starts_with, 1 );
        # TODO: standardize storage building routines
        my $index_file = "$path/$file";
        # Standardize "index key from file" routine
        return ( -f $index_file
            ? (
                $as_file
                ? "$path/$file"
                : $file # TODO translate back to key name
            ) : ()
        );
    } # otherwise...

    # Build a base path from $self->index_chunk_length sized chunks (but only
    # where those chunks are complete)
    my $index_value = $self->standardize_index_name( $starts_with );
    my @start_chunks;
    my $partial = "";
    my $chunks       = $self->index_chunks;
    my $chunk_length = $self->index_chunk_length;
    START_CHUNK: for ( my $i = 0; $i < $chunks; $i++ ) {
        my $chunk = substr(
            $index_value,
            $i * $chunk_length,
            $chunk_length,
        );
        if ( length( $chunk ) < $chunk_length ) {
            # Grab the remainder, if any.
            $partial = $chunk;
            last START_CHUNK;
        }
        push( @start_chunks, $chunk );
    }

    my $start_path = $self->storage_path(
        $self->index_path,
        $self->standardize_index_name( $index ),
        @start_chunks,
    );
    return () unless -d $start_path;

    my @search_paths;
    if ( length( $partial ) ) {
        opendir( my $dh, $start_path )
            or croak "Unable to open path for searching";
        DIR_SCAN: while (my $dir = readdir( $dh ) ) {
            next DIR_SCAN if $dir =~ m/\A \.{1,2} \Z/x
                          or $dir !~ m/\A \Q $partial \E/x;
            push( @search_paths, join( "/", $start_path, $dir ) );
        }
        closedir( $dh );
    } else {
        push( @search_paths, $start_path );
    }

    # Use File::Find to accumulate a list of keys
    # Use simple matching in the "wanted" sub:
    #   is a file
    #   the left N most characters of the filename must match $starts_with
    #   ends with '.idx'
    #
    my @matches;
    # TODO: parameterize ".idx" ?  At least standardize it, too many instances
    # of the magic string.
    my $suffix = $self->index_suffix;
    if ( defined $suffix ) {
        $suffix =~ s/(?<!\.)$suffix\Z/\.$suffix/;
    } else {
        $suffix = "";
    }
    find(
        { wanted => sub {
               -f $_
            && $_ =~ m/\A \Q$index_value\E .* \Q$suffix\E \Z/sx
            && push( @matches, $_ );
        } }, 
        @search_paths,
    );

    # Convert the keys back into strings
    # Return the strings
    my @strings = map {
        s/\Q$suffix\E\Z// if ( $suffix );
        pack( "H*", $_ );
    } @matches;

    return @strings;
}

# Given an index entry, return the list of UUIDs it contains.
sub lookup_by_index {
    my ($self, $index, $value) = @_;
    # Find exact match
    my $match = $self->search_index( $index, $value, 1, 1 );
    return () unless $match;
    # With the exact match confirmed, open the file and slurp the contents.
    my %data;
    open( my $fh, "<", $match ) or croak "Could not open index file for reading";
    while (my $entry = <$fh>) {
        chomp $entry;
        $data{$entry} = 1;
    }
    close( $fh );
    return keys %data;
}

1;
