package UUIDB::Storage::Fileplex;

=head1 NAME

UUIDB::Storage::Fileplex - File based document storage engine with index support

=head1 SYNOPSIS

    # Instantiated by UUIDB as part of general database setup.
    my $uuidb = UUIDB->new(
        document_type   => "JSON",     # <= Whatever; we don't care.
        storage_type    => "Fileplex", # <= that's us.
        storage_options => {
            path => "/path/to/database", # <= See "OPTION ATTRIBUTES"
        },
    );

    # Et voila!  There's now a JSON document on disk, retrievable later via key.
    my $key = $uuidb->create({
        this => "is",
        some => "data",
    });

    # Get another copy of the data (not shared ref!)
    my $data = $uuidb->get( $key );

    # Get the UUIDB::Document object
    my $document = $uuidb->get_document( $key );

    # Yup.
    is_deeply( $data, $document->data );

=head1 DESCRIPTION

The "plex" in "Fileplex" comes from "plexus" as the notion of an interwoven mass
or network (rather than the mathematical term for a complete system of equations
for expressing relationships between quantities, but it wouldn't be too much of
a stretch for that).  The interweaving aspect comes from how it decides to store
the UUIDB documents on disk.

Documents are stored one-to-a-file, named for the UUID assigned by the database
engine.  In order to avoid complications with overloading inodes or making a
directory untenable for maintenance it breaks them up a little according to a
simple (but extensible) encoding scheme, paring off C<plex_chunk> pieces
(default 3) of the UUID value of <plex_length> each (default 2).  For example:

    A JSON document with the following UUID:
    d35fd9d9-b899-440a-a8d2-07d7f0675f15

    Would be stored in the following path:
    d3/5f/d9/d35fd9d9-b899-440a-a8d2-07d7f0675f15.json
    ^1 ^2 ^3 ^ Full UUID                         ^ Suffix

...where the suffix value comes from the L<UUIDB::Document/suffix> value.

Advanced usage super-power: because of the way documents are stored on disk and
the predictable nature of plexing hexadecimal characters, it's entirely possible
to pre-populate the L</data_path> and/or L<index_path> directory trees with
symlinks to other locations (e.g., network storage).  It also plays well with
version control systems like Git.

=cut

use v5.10;
use strict;
use warnings;

# What we are...
use Moo;

# ...what we need.
use Carp            qw( carp croak );
use Digest::MD5     qw( md5_hex    );
use File::Find      qw( find       );
use IO::All         qw( -utf8      );
use Types::Standard qw(
    ArrayRef  Bool
    InstanceOf Int
    Maybe  Ref Str
);
use UUIDB::Util     qw( check_args );

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

# Prototypes for internal consistency enforcement.  These belong to functions
# which are scrubbed from the namespace, but that doesn't mean we're not
# civilized about the composition of our internal routines.

sub build_chunked_path ( $$$;$ );
sub is_empty_dir       ( _     );
sub prune_file         ( $;$   );
sub prune_tree         ( $;$   );
sub read_index_file    ( _     );
sub remove_index_file  ( _     );
sub write_index_file   ( $$@   );

=head1 OPTION ATTRIBUTES

These attributes are available to be set as options during instantiation, and
affect the configuration and behavior of file storage, indexing, etc.  Some of
these may only be set during instantiation (which is good, because changing them
after the fact could render the data inaccessible).  These will be marked as
C<read only> in the description, but may still be set by passing them to
L<UUIDB#storage_options>.

=head2 data_path

    data_path => "data", # Optional, defaults to "data"; read only

Simple string, directory name: specifies where under the Fileplex path the
documents will be stored.  Since this becomes part of the path itself, it should
be set to something compatible with the filesystem.  This directory will be
created if it does not exist.  See also L</path>.

=cut

has data_path => (
    is      => "ro",
    isa     => Str,
    default => "data",
);

=head2 index

    # ...
    index => [ "first_name", "geo" ], # Optional, must be an arrayref of strings
    # ...

    # With this storage_option set during instantiation, we can now do the
    # following:

    my @documents = $uuidb->storage->search_index( first_name => "Inigo" );
    # If there are any documents for which $document->extract("name") yields a
    # value beginning with "Inigo", those will now be in the @documents
    # collection returned from the search. Tada!

    # Get everyon from Florin.  This might be a large list - be careful about
    # what you index, the more unique the better.  Also, don't let anyone from
    # Guilder have this list.
    my @other_documents = $uuidb->storage->search_index( geo => "Florin" );

Probably the most significant feature of this storage engine are the indexes,
which are used to retrieve documents by values other than their UUID.  The
C<index> names here correspond to fields which can be retrieved from
L<UUIDB::Document> objects by use of the L<UUIDB::Document#extract> method.  The
index name and the extracted value will form a key-value pair and be written in
and inverse lookup array using fairly rudimentary association.  The match of a
value within a given key will then yield the UUID of the original record.

B<Limitations>: these really are simple inverse C<< value => key >> lookups, and
do not allow for full text search or even mid-value searching.  Inverse lookups
I<can> match on partial values, but only based on the beginning of the value;
they're I<really> fast for "begins with..." or exact match searches, but rubbish
for everything else.

Indexed values are case sensitive, and in fact need not even be string data:
any data type (and/or encoding) is supported in both the name of the C<index>
and the C<value> from the document.  Maximum length is determined by
L</index_length_max> (which comes with important caveats, so read that section),
and extremely long values don't make much sense for indexing in the first place
since you only need something sufficiently differentiated to be identifiable.

B<WHICH MEANS>: if you're going to be storing on definite articles, it makes
sense to apply Title indexing rules: C<lastname, firstname>, or C<Book, The>,
etc.  B<This is not provided for you>, because how data is mangled should be up
to you; it's just highly recommend that some intelligent form of mangling be
done.

See also L</search_index>, L</standardize_index_value>.

=cut

has index => (
    is      => 'rw',
    isa     => ArrayRef[Str],
    default => sub { [] },
);

=head2 index_chunks

    index_chunks => 3, # Optional, defaults to 3; read only

Like L</plex_chunks>, but for storing indexes.  This is a mostly-internal
option, and should be left alone unless you really know what you're doing.

=cut

has index_chunks => (
    is      => "ro",
    isa     => Int,
    default => 3,
);

=head2 index_chunk_length

    index_chunk_length => 2, # Optional, defaults to 2; read only

Like L</plex_chunk_length>, but for storing indexes.  Also not recommended for
modification unless you really know what you're doing; note that the use of wide
character values in which first hex bytes are likely to be C<0x00> will vastly
diminish variability in the plexing scheme.  In that case, changing this to 4
(or even 8) might make more sense.  As a general rule, "as many bytes as it
takes to represent a single character, times 2 (for hexadecimal translation)".

=cut

has index_chunk_length => (
    is      => "ro",
    isa     => Int,
    default => 2,
);

=head2 index_length_max

    index_length_max => 128, # Optional, defaults to 128

When indexes are created on the filesystem, they do so as a plex-chunked path
leading to a full-length file (see C<DESCRIPTION>).  In order to retain complete
compatibility with filesystems and various character encoding schemes, and not
have to worry about interpretation or misinterpretation of path-influencing
characters, this is done by using the hexadecimal representation of the raw
bytes of the value. This length limit I<refers to the length of the hexadecimal>
string, rather than the originating string (not including the L</index_suffix>
if any).  Character encodings can have a B<huge> influence on how much "data"
really makes it into the index, then.

Setting this value to C<0> disables max length enforcement, but isn't really
recommended.

=cut

has index_length_max => (
    is      => "ro",
    isa     => Int,
    default => 128,
);

=head2 index_path

    index_path => "index", # Optional, defaults to "index"; read only

Like L</data_path>, but for storing indexes.  Simple string, directory name:
specifies where under the database L</path> the indexes will be stored.

=cut

has index_path => (
    is      => "ro",
    isa     => Str,
    default => "index",
);

=head2 index_suffix

    index_suffix => "idx", # Optional, defaults to "idx"; read only

Indexed fields are stored as a simple file with a list of all matching UUIDs.
While it's just text, it's still nice to give it some indication of its purpose
and to differentiate it from other data.  This suffix is a convenient way of
doing just that.  Note that for sanity purposes this string must be 32
characters or less in length.

=cut

has index_suffix => (
    is      => "ro",
    isa     => Str,
    default => "idx",
);

=head2 overwrite_newer

    overwrite_newer => 0, # Optional boolean, defaults to 0

Document metadata includes a last modified timestamp from the filesystem on
which it's stored.  If, when we go to save an updated version of the document,
the timestamp on disk is newer than the one in the document's metadata, we have
a potential version conflict (someone else has updated it between loading our
version and starting the save operation).

The default behavior if that potential conflict is detected is to C<croak> with
an error message indicating the case.  If however this option is set to true, we
will proceed with the overwrite and merely C<carp> instead.

The safest resolution is to load the newer document and compare changes, but as
this may be a nuanced and costly operation (we're supposed to know about
storage, not documents), that's left up to the caller to sort out.

=cut

has overwrite_newer => (
    is      => "rw",
    isa     => Bool,
    default => 0,
);

=head2 path

    path => "/path/to/database", # Required, string (directory path); read only

This is a file based storage engine, so we need a place to store the files.
That place is determined by this string.  We will attempt to create the path if
it does not exist (inclusive of the entire directory tree), and will croak if we
are either unable to do that, or the provided path is not writable.  All
database content will be stored in subdirectories under this path as determined
by L</data_path> and L</index_path>.

=cut

has path => (
    is => "ro",
    isa => Str,
);

=head2 plex_chunks

    plex_chunks => 3, # Optional, defaults to 3; read only

Per the "plex" reference in the L</DESCRIPTION>, files are stored in nested
directories based on portions of the UUID for a given document:

    A JSON document with the following UUID:
    d35fd9d9-b899-440a-a8d2-07d7f0675f15

    Would be stored in the following path:
    d3/5f/d9/d35fd9d9-b899-440a-a8d2-07d7f0675f15.json
    ^1 ^2 ^3 ^ Full UUID                         ^ Document Suffix (if any)

Internally, the division of the UUID into path elements is referenced to as
"chunks", rather than something fancy like "octets", simply because they might
I<not> be an octet, or a byte, etc., as the length is variable (see also
L</plex_chunk_length>).  The number of chunks to be used is 3, as shown here.
Fewer than that and the pathing won't be as deep; more, and it will provide
further fragmentation/distribution.  3 is a sane default - you're welcome to
fiddle around, but are unlikely to see significant benefits moving either
direction.

The L</rehash_key> setting/algorithm can be used to augment or ensure uniqueness
within the first L</plex_chunks> count of L<plex_chunk_length> hexadecimal
characters if the UUID engine in use (see L<UUIDB#uuid_generator>) does not
provide suitable differentiation which might otherwise frustrate this scheme
(such as sequential, or provider prefixed values).

=cut

has plex_chunks => (
    is      => "ro",
    isa     => Int,
    default => 3,
);

=head2 plex_chunk_length

    plex_chunk_length => 2, # Optional, defaults to 2

Like L</index_chunk_length>, but for the storage of document data.  Not
recommended for modification unles you really know what you're doing, and with
the potential to make previously stored data inaccessible if used inconsistently
over the same data (meaning, once you decide on this value, forever will it
dominate your database).

Revisiting the example from L</DESCRIPTION> and L</plex_chunks>, altering this
value to say, C<3>, would change this:

    d3/5f/d9/d35fd9d9-b899-440a-a8d2-07d7f0675f15.json

To this (assuming L<plex_chunks> is still C<3>):

    d35/fd9/d35/fd9d9-b899-440a-a8d2-07d7f0675f15.json

But would also mean changing the number of potential entries in a given
directory level from 256 (hexadecimal ^ 2) to 4096 (hexadecial ^ 3), and may
aversion affect storage performance.  Reducing this (to hexidecimal ^ 1) would
also cause issues with too many entries showing up in the latter trees.  Those
needed I<less> differentiation, especially in the earlier byte segments (such as
mapping to a limited number of NFS mounts) are recommended to pre-populate the
expected ranges with symlinks to appropriate locations instead, and just rely
on those.

=cut

has plex_chunk_length => (
    is      => "ro",
    isa     => Int,
    default => 2,
);

=head2 rehash_key

    # ...
    rehash_key => 1, # Optional, defaults to false
    # ...

Useful when we need to take the document's UUID and turn it into something with
greater variability (e.g., when using sequential or fixed-prefix UUID providers)
since we rely on differentiating the first few bytes of the key for the shard
storage management (plexing; see L</DESCRIPTION>).

It is I<highly> recommended to use the L<UUIDB::Document/propagate_uuid>
setting when this is enabled, since otherwise the reverse association from
hashed key to UUID is effectively impossible to determine (meaning, if we don't
store the document's UUID in the data of the document, it's essentially
orphaned).

=cut

has rehash_key => (
    is      => 'rw',
    isa     => Bool,
    default => 0,
);

=head1 ATTRIBUTES

These attributes are for the use and operation of the storage engine itself.
Tread lightly, and favor diagnostics over modification.

=head2 initialized

Boolean to indication whether the storage engine has been properly initialized,
checking all settings, creating needed directories, etc.  These operations are
not attempted until the first level of read/write request demands them, in order
to be late binding and thus save on instantiation overhead.  Those wishing to
make these assertions earlier rather than later (in order to fail quickly) can
call L</init_check> to do so.

=cut

has initialized => (
    is      => 'rwp',
    isa     => Bool,
    default => 0,
);

=head1 STANDARD METHODS

These are methods inherited from L<UUIDB::Storage> which we implement or extend.
The nature of the implementation will be covered here, but where it fits into
the larger L<UUIDB> ecosystem is better covered by checking the base modules.

Generally speaking it's probably better to access these via their handles on a
L<UUIDB> instance rather than on a C<UUIDB::Storage::Fileplex> instance
directly.

=head2 delete_document

    $uuidb->delete( $uuid, $warnings );

Permanently removes a document from data storage and any index entries which
link to it.

If C<$warngins> is true and the document does not exist a small warning will be
generated (in case someone needs to determine why a document we expected to act
upon cannot be found, even if only to delete it).

We will also complain if there are L</index> entries and the document cannot be
loaded, regardless of C<$warnings>.  Why load a document during a delete cycle?
Because we have to retrieve any data elements used for reverse indexing in order
to purge those as well.

We also try to be nice and tidy in the process - any empty directory fragments
in the tree will also be pruned.

=cut

sub delete_document {
    my ($self, $uuid, $warnings) = @_;
    if ( my ($path) = $self->document_exists( $uuid ) ) {
        # Index references to this document need to be removed as well; rather
        # than scan all indexes to see which refer to this UUID, we can load the
        # document in question and use it to hunt them down much less
        # expensively (no full directory scans and greps).
        if (scalar @{ $self->index }) { # Do we have indexes?
            my $document = eval { $self->get_document( $uuid ) };
            carp $@ if $@ && $warnings; # TODO: Keep?
            if ( $document ) {
                $self->update_indexes( $document, 1 ); # "clear all" mode
            } else {
                carp "Unable to load document during delete, indexes will not be purged";
            }
        }

        # Remove the document
        return prune_file $path, $self->path;
    } elsif ( $warnings ) {
        carp "Document not found, nothing deleted";
    }
}

=head1 document_exists

    if ( $uuidb->document_exists( $uuid ) ) {
        # Do stuff.
    }

Simple boolean indicating whether a document by the given C<$uuid> can be found
on disk.  As a minor extension to the original spec, if invoked in list context
the path of the identified document will be returned instead of just the
boolean, which is useful for not having to repeat operations.

=cut

sub document_exists {
    my ($self, $uuid) = @_;
    check_args(
        args => { uuid => $uuid },
        must => { uuid => Str   }, # TODO: is_uuid_string ?
    );
    my $suffix = $self->db->default_document_handler->suffix;
    my $path   = $self->compose_document_path( $uuid, $suffix, 1 );
    unless ( -f $path ) {
        my $found_alt = 0;
        if ( $suffix ) {
            # Let's see if there's a version by a different suffix; but if there is,
            # we're going to make noise about it.
            my ($path_plain) = ( $path =~ m/\A (.*) \. \Q$suffix\E \Z/x );
            if (my @found = glob "$path_plain*") {
                carp "Did not find document $uuid with expected suffix $suffix, "
                    ."but did find " . scalar( @found ) . " candidates: "
                    .join( ", ", @found );
                $path = shift @found;
                $found_alt = 1;
            }
        }
        return unless $found_alt;
    }
    return ( wantarray ? $path : 1 );
}

=head2 get_document

    my $document = $uuidb->get_document( $uuid, $optional_document_handler );

Return a L<UUIDB::Document> instance (most likely a descendant thereof) for the
given C<$uuid>.  The optional second argument must be an instance of
L<UUIDB::Document> if passed, and will be used to determine the type and assist
in the retrieval and hydration of the identified document.

=cut

sub get_document {
    my ($self, $uuid, $document_handler) = @_;
    check_args(
        args => {
            uuid             => $uuid,
            document_handler => $document_handler,
        },
        must => { uuid => Str },
        can  => { document_handler => InstanceOf[qw( UUIDB::Document )] },
    );
    my ($path) = $self->document_exists( $uuid );
    return unless $path;

    # Lock, open, read, close.
    my $data_file = io->file($path)->lock;
    my $ctime = $data_file->ctime;
    my $data  = $data_file->all;
    $data_file->close; # and implicit unlock

    $document_handler ||= $self->db->default_document_handler;
    my $document = $document_handler->new_from_data(
        $document_handler->thaw( $data )
    );
    # The document creation might be taking care of this for us, but if UUID
    # propagation isn't turned on we'll need to do this manually.  Would be nice
    # if it was more automatic than this, but there's a fine line between
    # "magic" and "too much magic what were you thinking".  Maybe we'll add an
    # event model later.
    $document->uuid( $uuid )
        unless $document->uuid()
        and    $document->uuid() eq $uuid;

    $document->meta->{ctime}      = $ctime;
    $document->meta->{in_storage} = 1;

    # Metadata entry for the current values under which the document is indexed.
    # If these values change, we'll need to clean up the old indexes before
    # saving the new ones.  We'll also need to use this to accurately purge
    # indexes during document delete.
    my @indexes = @{ $self->index };
    if ( scalar @indexes ) {
        $document->meta->{indexed} = $document->extract( @indexes );
    }

    return $document;
}

=head2 standardize_key

    my $standardized_uuid = $fileplex->standardize_key( $uuid );

Since byte storage tends to be choosy about things like case sensitivity and the
like, all keys are standardized to the same style for storage, comparison, etc.
In the case of L<UUIDB::Storage>, that means simply verifying its UUID-ness and
putting it in lowercase.  Here we add the additional option of re-hashing the
key, if the L</rehash_key> storage option is set.  If it is,
L</rehash_algorithm> will be invoked to do its thing.

=cut

sub standardize_key {
    my ($self, $key) = @_;
    # This also does an is_uuid_string check for us.
    $key = $self->SUPER::standardize_key( $key );
    if ( $self->rehash_key ) {
        $key = $self->rehash_algorithm( $key );
    }
    return $key;
}

=head2 store_document

    my $document_uuid = $uuidb->store_document( $a_document );

Writes a L<UUIDB::Document> instance to storage, managing any indexes along the
way (see the L</index> storage option).  Overwrites any existing document by the
same UUID (technically, same UUID and same L<UUIDB::Document#suffix>), but first
checks to see whether that document looks "newer" than ours based on C<ctime>
comparison.  If it does, we'll C<croak> with an error message unless the
L</overwrite_newer> storage option is set, in which case we'll merely C<carp>
and hope you know what you're doing while we pave it over.

=cut

sub store_document {
    my ($self, $document) = @_;

    check_args(
        args => { document => $document                         },
        must => { document => InstanceOf[qw( UUIDB::Document )] },
    );

    $self->init_check();

    my $data = $document->frozen;
    unless (defined $data) {
        carp "No document content; nothing to store (did you mean 'delete'?)";
        return;
    }

    my ($document_path, $filename) = $self->compose_document_path(
        $document->uuid,
        $document->suffix,
        1, # full system path
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
            # TODO: callback for making comparisons and/or deciding course of
            # action?

            # Overwrite warnings (if the document has been updated more recently
            # than the local $document copy believes it has.
            unless ( $self->overwrite_newer ) {
                croak "Not overwriting document with newer timestamp than ours";
            }
            carp "Overwriting record with newer timestamp than ours";
        }
    }
    $docfile->open(">");
    $docfile->print( $data );
    $docfile->close();
    $document->meta->{ctime} = $docfile->ctime;
    $document->meta->{in_storage} = 1;

    # Process indexes.
    $self->update_indexes( $document );

    return $document->uuid();
}

=head1 CUSTOM METHODS

These methods expand the basic storage signature for its own purposes, mostly
navigating and maintaining indexes or other internal housekeeping.

=head2 init_check

    $fileplex->init_check();

Verifies all settings for sanity and asserts basic storage path existence and
writeability (creating it as needed).  Does not return anything, but will croak
(loudly) if there are issues.  After successful one-time initialization this
becomes a noop.

=cut

sub init_check {
    my ($self) = @_;

    return if $self->initialized;

    my $gt_zero   = sub { shift >  0     };
    my $wordy_str = sub { shift =~ m/\w/ };
    check_args(
        args => {
            map {( $_ => $self->$_() )} (qw(
                data_path
                index_path
                index_chunks
                index_chunk_length
                index_suffix
                path
                plex_chunks
                plex_chunk_length
            ))
        },
        must => {
            data_path          => $wordy_str,
            index_path         => $wordy_str,
            index_chunks       => $gt_zero,
            index_chunk_length => $gt_zero,
            index_suffix       => [ $wordy_str, sub { length( shift ) <= 32 } ],
            path               => $wordy_str,
            plex_chunks        => $gt_zero,
            plex_chunk_length  => $gt_zero,
        },
    );

    croak "Path not set" unless $self->path;
    unless ( -d $self->path ) {
        croak "Invalid path (not found)"
            unless io->dir( $self->path )->mkpath;
    }
    croak "Path not writable"
        unless -w $self->path
        and      !$self->readonly;

    $self->_set_initialized( 1 );
}

=head1 compose_document_path

    my $document_path = $fileplex->compose_document_path(
        $uuid,    # UUID of the document whose path we're composing
        $suffix,  # suffix to append
        $full,    # bool: return a full system path, or relative to $self->path?
    );

Creates the path under which a given UUID can be stored in the system.  Does not
look for collisions or verify writeability, just assembles appropriate pieces.
This is used during other storage and retrieval operations to tell us where to
put it or find it, and consumes the various plex settings (L</plex_chunks>,
L<plex_chunk_length>).

The C<$suffix> option tells us what suffix if any should be used when
constructing the path (and unlike some other operations, does NOT fall back to
the L<UUIDB#default_document_handler> to get the suffix if not provided, making
it suitable for other manual operations.

The C<$full> option prepends the L</path> of the database, otherwise paths will
be relative to that starting point.

If used in list context, will return the directory first and filename second. In
scalar context will concatenate those into one.

=cut

sub compose_document_path {
    my ($self, $uuid, $suffix, $full) = @_;

    check_args(
        args => {
            uuid   => $uuid,
            suffix => $suffix,
            full   => $full,
        },
        must => {
            uuid => Str,
        },
        can => {
            suffix => Str,
            full   => Bool,
        },
    );

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

=head2 rehash_algorithm

    my $rehashed_uuid = $fileplex->rehash_algorithm( $uuid );

Given a C<$uuid>, perform some magic on it to make it more suitable for use in
plexing (mostly by ensuring sufficient variability within the first
C<< L<plex_chunks> * L<plex_chunk_length> >> characters.  Standard
implementation is just C<md5_hex>, which should fit the bill.  Extend as you see
fit to use an alternate algorithm.

Note that this is invoked during L</standardize_key> I<only> if the
L</rehash_key> boolean is also set, otherwise it's ignored.  Calling it directly
won't be a noop though, so it's probably best to rely on L</standardize_key> to
keep the logic consistent.

=cut

sub rehash_algorithm {
    my ($self, $key) = @_;
    return md5_hex( $key );
}

=head2 storage_path

    my $path = $fileplex->storage_path( $optional, $additional, $dirs );

Creates a simple directory tree based on L</path> plus whatever else is passed
in (if anything).  This is just a DRY feature to avoid repetitive manual
concatenation in other places where we need to assemble paths.

=cut

sub storage_path {
    my ($self, @dirs) = @_;
    $self->init_check();
    return join( "/", $self->path, @dirs );
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
#   index would be stored.
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

# TODO: when doing unit tests for this, double check that when changing an
# indexed value, the old index is removed.
sub update_indexes {
    my ($self, $document, $clear_all) = @_;
    check_args(
        args => {
            document  => $document,
            clear_all => $clear_all,
        },
        must => { document  => InstanceOf[qw( UUIDB::Document )] } ,
        can  => { clear_all => Bool },
    );

    my $uuid    = $document->uuid;
    my @indexes = @{ $self->index };
    my %indexed = %{ $document->meta->{indexed} || {} };
    if ( scalar @indexes ) {
        # TODO: This is a slow approach; might be better if we built a batch
        # index update process.
        my %values = $document->extract( @indexes );
        DOC_INDEX: foreach my $index ( @indexes ) {
            my $value = $values{ $index };

            if ( exists $indexed{ $index } ) {
                # Remove it from the old index IF: the values have changed OR
                # we're in clear_all mode.  And if we're in clear_all mode, we
                # can skip ahead to the next one.
                if ( $clear_all || $indexed{ $index } ne $value ) {
                    $self->clear_index( $index, $indexed{ $index }, $uuid );
                }

                # If the indexed value hasn't changed, or we've just cleared out
                # an old reference and don't need to write a new one, skip
                # ahead.
                next DOC_INDEX
                    if $clear_all
                    or $indexed{ $index } eq $value;
            }

            # If there's something in the index field, save it.
            # If not, or we're in clearing mode, clear it.
            my $mode = (
                !$clear_all
                && defined $value
                && length  $value
                ?  'save_index'
                :  'clear_index'
            );
            $self->$mode( $index, $value, $uuid );
        }
        # Update the index metadata with the current snapshot.
        if ( $clear_all ) {
            delete $document->meta->{indexed};
        } else {
            $document->meta->{indexed} = \%values;
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
    my $maxlength = $self->index_length_max;

    check_args(
        args => { index => $key         },
        must => { index => [Str, qr/./] },
    );
    if ($maxlength) {
        # We know the value is going to be multiplied by at least 2 when it goes
        # through the hex packing below; it doesn't make sense to process more
        # data than we need; thus, truncate the data at this point (with a
        # smidgeon of padding) just in case.
        $key = substr( $key, 0, int( ( $maxlength / 2 ) + 1 ) );
    }

    my $hexkey = unpack( "H*", $key );
    if ($maxlength) {
        $hexkey = substr( $hexkey, 0, $maxlength );
    }
    return $hexkey;
}

sub standardize_index_name {
    my ($self, $index_name) = @_;
    return $self->index_key( $index_name );
}

# Simple pass-through, but if you want to add definite article rules
# "Lastname, Firstname", or "Name of Book, The", this is the place to do it.
sub standardize_index_value {
    my ($self, $index_name, $index_value) = @_;
    return $index_value;
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
    for ($suffix) {
        last unless defined;
        s/(?<!\.)$suffix\Z/\.$suffix/;
    }

    my $match_key = $self->index_key( $starts_with );

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
        my $index_value  = $self->index_key( $starts_with );
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

sub prune_file ($;$) {
    my ($path, $stop) = @_;

    unlink $path or carp "Could not remove file";

    # prune the directory tree if that was the only file in it (and so on)
    $path =~ s{/[^/]+\Z}{};
    prune_tree( $path, $stop );

    return 1;
}

sub prune_tree ($;$) {
    my ($dir, $stop) = @_;

    croak "Invalid directory"
        unless    $dir
        and    -d $dir;

    my $loop_dir = $dir;
    $loop_dir =~ s{/*\Z}{}; # Remove any trailing slash
    PRUNE: while (1) {
        # Technically we could just do the rmdir instead of checking for
        # emptiness first, but we're trying to be nice.
        last PRUNE if $stop and $loop_dir eq $stop;
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

    # 1. Load the existing index, if any
    # 2. Add the new UUID to the list
    # 3. Write the list back to the index
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
