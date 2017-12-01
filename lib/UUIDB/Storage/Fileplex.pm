package UUIDB::Storage::Fileplex;

=head1 NAME

UUIDB::Storage::Fileplex - File based document storage engine with index support

=head1 SYNOPSIS

    # Instantiated by UUIDB as part of general database setup.
    my $uuidb = UUIDB->new(
        document_type   => "JSON",     # <= Whatever; we don't care.
        storage_type    => "Fileplex", # <= that's us.
        storage_options => {
            path => "/path/to/database", # <= See "set_options"
        },
    );

    # Et voila!  There's now a JSON document on disk, retrievable later via key.
    my $key = $uuidb->create({
        this => "is",
        some => "data",
    });

=head1 DESCRIPTION

The "plex" in "Fileplex" comes from "plexus" as the notion of an interwoven mass
or network (rather than the mathematical term for a complete system of equations
for expressing relationships between quantities, but it wouldn't be too much of
a stretch for that).  The interweaving aspect comes from how it decides to store
the UUIDB documents on disk.

Documents are stored one-to-a-file, named for the UUID assigned by the database
engine.  In order to avoid complications with overloading inodes or making a
directory untenable for maintenance it breaks them up a little accroding to a
simple (but extensible) encoding scheme, paring off C<plex_chunk> pieces
(default 3) of the UUID value of <plex_length> each (default 2).  For example:

    A JSON document with the following UUID:
    d35fd9d9-b899-440a-a8d2-07d7f0675f15

    Would be stored in the following path:
    d3/5f/d9/d35fd9d9-b899-440a-a8d2-07d7f0675f15.json
    ^1 ^2 ^3 ^ Full UUID                         ^ Suffix

...where the suffix value comes from the L<UUIDB::Document/suffix> value.

=cut

use v5.10;
use strict;
use warnings;

use Carp qw( carp croak );

use Moo;
use Digest::MD5     qw( md5_hex    );
use File::Find      qw( find       );
use File::Path      qw( mkpath     );
use IO::All         qw( -utf8      );
use UUIDB::Util     qw( check_args );
use Types::Standard qw(
    ArrayRef  Bool
    InstanceOf Int
    Maybe  Ref Str
);

# Be tidy.
use namespace::autoclean -also => [qw(
    build_chunked_path
    is_empty_dir
    prune_file
    prune_tree
    read_index_file
    remove_index_file
    write_index_file
)];

extends qw( UUIDB::Storage );

# Prototyes for internal consistency enforcement.  These belong to functions
# which are scrubbed from the namespace, but that doesn't mean we're not
# civilized about the composition of our internal routines.
sub build_chunked_path ( $$$;$ );
sub is_empty_dir       ( _     );
sub prune_file         ( _     );
sub prune_tree         ( _     );
sub read_index_file    ( _     );
sub remove_index_file  ( _     );
sub write_index_file   ( $$@   );

=head1 OPTION ATTRIBUTES

These attributes are available to be set as options during instantiation, and
affect the configuration and behavior of file storage, indexing, etc.

=cut

# TODO: POD

has data_path => (
    is      => "rw",
    isa     => Str,
    default => "data",
);

has path => (
    is => "rw",
    isa => Str,
);

has plex_chunks => (
    is      => "rw",
    isa     => Int,
    default => 3,
);

has plex_chunk_length => (
    is      => "rw",
    isa     => Int,
    default => 2,
);

has index_chunks => (
    is      => "rw",
    isa     => Int,
    default => 3,
);

has index_chunk_length => (
    is      => "rw",
    isa     => Int,
    default => 2,
);

has index_path => (
    is      => "rw",
    isa     => Str,
    default => "index",
);

has index_suffix => (
    is      => "rw",
    isa     => Str,
    default => "idx",
);

has overwrite_newer => (
    is      => "rw",
    isa     => Bool,
    default => 0,
);

=head2 rehash_key

Advanced setting; boolean or code reference:

    # ...
        rehash_key => 1, # Turn on, use default rehash algorithm
        rehash_key => \&some_code, # Turn on, use code to do rehashing.
    # ...

Useful when we need to take the document's UUID and turn it into something with
greater variability (e.g., when using sequential or fixed-prefix UUID providers)
since we rely on differentiating the first few bytes of the key for the shard
storage management (plexing; see L</DESCRIPTION>).

It is I<highly> recommended to use the L<UUIDB::Document/propagate_uuid>
setting when this is enabled, since otherwise the reverse association from
hashed key to UUID is effectively impossible to determine (meaning, if we don't
store the document's UUID in the data of the document, it's effectively
orphaned).

=cut

has rehash_key => (
    is      => 'rw',
    isa     => Maybe[Bool, Ref[qw( CODE )]],
    default => 0,
);

=head1 ATTRIBUTES

These attributes are for the use and operation of the storage engine itself.

=cut

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

sub init_check {
    my ($self) = @_;
    croak "Path not set" unless $self->path;
    unless ( -d $self->path ) {
        croak "Invalid path (not found)";
    }
    croak "Path not writable"
        unless -d $self->path # Also catches failures to auto-create above
        and    -w $self->path
        and      !$self->readonly;
}

sub init_store {
    my ($self) = @_;
    $self->init_check();

    mkpath( $_ ) for grep { ! -d } (
        $self->storage_path( $self->data_path  ),
        $self->storage_path( $self->index_path ),
    );
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
    unless ($data) {
        carp "No document content; nothing to store (did you mean 'delete'?)";
        return;
    }

    my ($document_path, $filename) = $self->compose_document_path(
        $document->uuid,
        $document->suffix,
        1,
    );

    # "assert" will auto create the path for us.
    my $docfile = io->file("$document_path/$filename")->assert->lock;
    # TODO: keep a "last error" string around, so people can actually double
    # check the message instead of trying to trap the signal?
    if (my $ctime = $docfile->ctime) {
        if (
               $document->meta->{ctime}
            && $document->meta->{ctime} < $ctime
        ) {
            # Overwrite warnings (if the document has been updated more recently
            # than the local $document copy believes it has.
            unless ( $self->overwrite_newer ) {
                carp "Not overwriting document with newer timestamp than ours";
                return;
            }
            carp "Overwriting record with newer timestamp than ours";
        }
    }
    $docfile->open(">");
    $docfile->print( $data );
    $docfile->close();
    $document->meta->{ctime} = $docfile->ctime;

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
    check_args(
        args => {
            uuid             => $uuid,
            document_handler => $document_handler,
        },
        must => {
            uuid => Str, # TODO: is_uuid_string ?
        },
        can => {
            document_handler => InstanceOf[qw( UUIDB::Document )],
        },
    );
    $document_handler ||= $self->db->default_document_handler;
    my $path = $self->exists( $uuid, $document_handler->suffix, 1 );
    return unless $path;

    # Lock, open, read, close.
    my $data_file = io->file($path)->lock;
    my $ctime = $data_file->ctime;
    my $data  = $data_file->all;
    $data_file->close; # and implicit unlock

    my $document = $document_handler->new_from_data(
        $document_handler->thaw( $data )
    );
    $document->uuid( $uuid )
        unless $document->uuid()
        and    $document->uuid() eq $uuid;

    $document->meta->{ctime} = $ctime;

    return $document;
}

sub exists {
    my ($self, $uuid, $suffix, $as_path) = @_;
    check_args(
        args => {
            uuid    => $uuid,
            suffix  => $suffix,
            as_path => $as_path,
        },
        must => { uuid   => Str }, # TODO: is_uuid_string ?
        can  => {
            suffix  => Maybe[Str],
            as_path => Bool,
        },
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
        # clear from various indexes, which requires loading first and doing the
        # index lookups.  But if we can't load it (because the file is bad, or
        # somehow corrupted, just emit a small warning and be on our way.
        my @indexes  = @{ $self->index };
        my $document = eval { $self->get_document( $uuid ) };
        carp $@ if $@ && $warnings; # TODO: Keep?
        if ( @indexes && !$document ) {
            carp "Unable to load document during delete, indexes will not be purged";
        } elsif ( scalar @indexes ) {
            my $values = $document->extract( @indexes );
            CLEAN_INDEX: foreach my $index ( @indexes ) {
                my $value = $$values{ $index };
                next CLEAN_INDEX unless defined $value
                                 and    length  $value;

                $self->clear_index( $index, $value, $uuid );
            }
        }

        # Remove the document
        return prune_file $path;
    } elsif ( $warnings ) {
        carp "Document not found, nothing deleted";
    }
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

sub storage_path {
    my ($self, @dirs) = @_;
    my $base_path = $self->path;
    croak "No base path set" unless $base_path;
    return join( "/", $base_path, @dirs );
}

sub rehash_algorithm {
    my ($self, $key) = @_;
    return md5_hex( $key );
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

=head1 Index Related Methods

=cut

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
    my $index_path = $self->compose_index_path( $index, $value, 1 );
    write_index_file( $index_path, 1, $uuid );
}

sub clear_index {
    my ($self, $index, $value, $uuid) = @_;
    check_args(
        args => {
            index => $index,
            value => $value,
            uuid  => $uuid,
        },
        must => {
            index => [ Str, qr/\A[^\s]+/ ],
            value => [ Str, qr/\A[^\s]+/ ],
            uuid  => [ Str ],
        },
    );
    $uuid = $self->SUPER::standardize_key( $uuid );
    # Do an exact index match on filename.
    if ( my $index_file = $self->search_index( $index, $value, 1, 1 ) ) {
        my @uuids = grep { $_ ne $uuid } read_index_file( $index_file );
        if ( scalar( @uuids ) ) {
            write_index_file( $index_file, 0, @uuids );
        } else {
            remove_index_file( $index_file );
        }
    }
}

sub compose_index_path {
    my ($self, $index, $key, $full) = @_;
    check_args(
        args => {
            index => $index,
            key   => $key,
            full  => $full,
        },
        must => {
            index => [Str, qr/./],
            key   => [Str, qr/./],
        },
        can => {
            full  => Bool,
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
# This is a lot of conditional args. It's likely only the first 2 are going to
# be used by any consuming program, but we should probably think about how we
# want to handle this a little more cleanly.
sub search_index {
    my (
        $self,
        $index,
        $starts_with,
        $exact_match,
        $as_path,
    ) = @_;

    check_args(
        args => {
            index       => $index,
            starts_with => $starts_with,
            exact_match => $exact_match,
            as_path     => $as_path,
        },
        must => {
            index       => [ Str, qr/\A[^\s]+/ ],
            # TODO: What's the minimum length required?
            starts_with => [ Str, qr/\A[^\s]+/ ],
        },
        can => {
            exact_match => Bool,
            as_path     => Bool,
        },
    );

    croak "Unknown index '$index'"
        unless grep { $_ eq $index } @{ $self->index };

    my $suffix = $self->index_suffix;
    if ( defined $suffix ) {
        $suffix =~ s/(?<!\.)$suffix\Z/\.$suffix/;
    } else {
        $suffix = "";
    }

    my @matches;
    if ( $exact_match ) {
        # Build the full path, return it if -f $full_path or undef otherwise.
        # Do this up front because it's a cheaper lookup than just changing the
        # directory regex below to be exact.
        my ($index_path, $filename) = $self->compose_index_path( $index, $starts_with, 1 );
        my $index_file = "$index_path/$filename";

        # Not found? Return false.
        return unless -f $index_file;

        # Found? Return the path, if that's what they want.
        return $index_file if $as_path;

        # Return the name of the identified index.
        push @matches, $filename;
    } else {
        # Build a base path from $self->index_chunk_length sized chunks (but only
        # where those chunks are complete)
        my $index_value  = $self->standardize_index_name( $starts_with );
        my @start_chunks;
        my $partial      = "";
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
        return unless -d $start_path;

        my @search_paths;
        if ( length( $partial ) ) {
            opendir( my $dh, $start_path )
                or croak "Unable to open path for searching";
            DIR_SCAN: while ( my $dir = readdir( $dh ) ) {
                next DIR_SCAN if $dir =~ m/\A \.{1,2} \Z/x
                              or $dir !~ m/\A \Q$partial\E/x;
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
        #   ends with the suffix, if specified
        #
        find(
            { wanted => sub {
                   -f $_
                && $_ =~ m/\A \Q$index_value\E .* \Q$suffix\E \Z/sx
                && push( @matches, $_ );
            } },
            @search_paths,
        );
    }

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
    return unless $match;
    return read_index_file( $match );
}

# Everything below this line should be namespace cleaned.

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

sub is_empty_dir (_) {
    my ($dir) = @_;
    croak "Invalid directory"
        unless    $dir
        and    -d $dir;

    my $empty = 1;
    opendir( my $dh, $dir );
    DIR_LOOP: while ( my $dir = readdir( $dh ) ) {
        next DIR_LOOP if $dir =~ m{\A \.\.? \Z}x;
        $empty = 0;
        last DIR_LOOP;
    }
    closedir( $dh );
    return $empty;
}

sub prune_file (_) {
    my ($path) = @_;

    unlink $path or carp "Could not remove file";

    # prune the directory tree if that was the only file in it (and so on)
    $path =~ s{/[^/]+\Z}{};
    prune_tree( $path );

    return 1;
}

sub prune_tree (_) {
    my ($dir) = @_;

    croak "Invalid directory"
        unless    $dir
        and    -d $dir;

    my $loop_dir = $dir;
    $loop_dir =~ s{/*\Z}{}; # Remove any trailing slash
    PRUNE: while (1) {
        # Technically we could just do the rmdir instead of checking for
        # emptiness first, but we're trying to be nice.
        last PRUNE unless is_empty_dir $loop_dir;
        last PRUNE unless rmdir        $loop_dir;
        $loop_dir =~ s{\A (.*)/.* \Z}{$1}x;
    }
}

sub read_index_file (_) {
    my ($file) = @_;
    croak "Invalid index file" unless -f $file;
    # With the exact match confirmed, open the file and slurp the contents.
    my %data;
    my $index = io->file($file)->chomp->lock;
    $index->open("<");
    while (my $entry = $index->getline) {
        $data{$entry} = 1; # dedupe
    }
    $index->close();
    return keys %data;
}

sub remove_index_file (_) {
    my ($path) = @_;

    # Nothing to do if it doesn't exist.
    return unless $path
           and -f $path;

    return prune_file $path;
}

sub write_index_file ($$@) {
    my ($path, $merge, @uuids) = @_;
    check_args(
        args => {
            path  => $path,
            merge => $merge,
            uuids => [ @uuids ],
        },
        must => {
            path  => Str,
            merge => Bool,
            uuids => ArrayRef[Str], # is_uuid_string?
        },
    );

    # TODO: Make this escape-character safe, just in case.  Even though we know
    # *our* files are OK, we don't want to create problems elsewhere.
    my ($storage_path, $filename) = $path =~ m{\A (.*) / (.*) \Z}x;

    # Load the existing index, if any
    # Add the new UUID to the list
    # Write the list back to the index
    my $index = io->file( $path )->assert->lock;
    if ( $merge && -f $path ) {
        push( @uuids, read_index_file( $path ) );
    }
    # Make sure the list is unique
    my %data = map { $_ => 1 } @uuids;
    $index->open(">");
    # TODO: benchmarking:
    $index->println($_) for keys %data;
    $index->close();
    return 1;
}

1;
