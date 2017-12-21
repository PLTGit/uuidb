package UUIDB;

=head1 NAME

UUIDB - A UUID key based document storage system.

=head1 VERSION

0.1

=head1 SYNOPSIS

    my $uuidb = UUIDB->new(
        name          => "Test",   # Helpful for differentiating multiple DBs.
        document_type => "JSON",   # Type of data serialization to use;
        storage_type  => "Memory", # Engine (and location) for storing it.
    );

    # JSON only because that's what we're demonstrating; could be anything.
    my %data = (
        any => "kind",
        of  => {
            serializable => [qw(
                data and stuff
            )],
        }
    );

    # Create it; $key is now a UUID string.
    my $key = $uuidb->create( \%data );

    # Read it
    my %data_copy = %{ $uuidb->get( $key ) };

    # Update it
    $data_copy{change} = "something";
    $uuidb->set( $key, \%data_copy );

    # Delete it
    $uuidb->delete( $key );

    # Double check.
    say "Nope" unless $uuidb->exists( $key );

    # Slightly more OO version
    # Create it (and save it)
    my $document = $uuidb->create_document( \%data );
    $key = $document->uuid();
    $document->data->{still} = "simple data";
    $document->save(); # or $document->update(); explicit store.

    if (my $document_copy = $uuidb->get_document( $key )) {
        is_deeply(
            $document_copy->data,
            $document->data,
            "Everything matches."
        );
    }

=cut

=head1 DESCRIPTION

UUIDB is a UUID (frequently called "GUID"s as well) based NoSQL document store
for Perl, ideal for graph storage, JSON DBs, etc.

Usage is as simple as loading the library and configuring the object - no other
processes or servers required (unless your chosen back-end is a DB engine or
similar).

Because unique identifiers are used for creating and managing keys, multiple
concurrent read access scales exceptionally well.  Concurrent write access is
fairly safe as well - definitely safe in the form of addition, I<mostly> safe
for modifications depending on the storage engine in use (see L<UUIDB::Storage>
for more information).

=cut

use v5.10;
use strict;
use warnings;

use Carp            qw( carp croak                              );
use Scalar::Util    qw( blessed                                 );
use Types::Standard qw( Any Bool HashRef InstanceOf Map Ref Str );
use UUID::Tiny      qw( create_uuid_as_string UUID_V4           );
use UUIDB::Util     qw( check_args is_loaded safe_require       );

use namespace::autoclean;
use Moo;

has name => (
    is  => 'ro',
    isa => Str,
);

has default_document_type => (
    is      => 'rw',
    isa     => Str,
    default => sub { "JSON" },
);

has document_handler => (
    is      => "rw",
    isa     => Map[Str, InstanceOf[qw( UUIDB::Document )]],
    default => sub { {} },
);

has storage => (
    is        => 'rw',
    isa       => InstanceOf[qw( UUIDB::Storage )],
    predicate => 1,
);

# Not a fan of sub { sub {} }, but it's the best way to do what we're doing,
# since we're passing args to create_uuid_as_string and therefor can'tjust point
# straight at it.
#
# WARNING: some UUID versions don't provide a lot of variability.  Not only is
# this cryptographically insecure, but it can lead to issues when it's being
# used for distribution of data in storage (UUIDB::Storage::Fileplex, for
# example, uses the first 3 octets for path resolution), and a lack of
# variability can lead to putting too many entries in one location.
has uuid_generator => (
    is      => 'rw',
    isa     => Ref[qw( CODE )],
    default => sub { sub { create_uuid_as_string( UUID_V4 ) } },
);

sub BUILD {
    my ($self, $args) = @_;

    # These methods provide their own validation.
    if ( $args->{document_type} ) {
        $self->document_type(
            $args->{document_type},
            ( $args->{document_options} ?
                %{ $args->{document_options} }
            : () ),
        );
    }
    if ( $args->{storage_type} ) {
        $self->storage_type(
            $args->{storage_type},
            ( $args->{storage_options} ?
                %{ $args->{storage_options} }
            : () ),
        );
    }
}

sub document_type {
    my ($self, $document_type, %opts) = @_;

    # If we're being called as an accessor, just give what we've got.
    return $self->default_document_type
        unless defined $document_type;

    check_args(
        args => {
            %opts,
            document_type => $document_type, # Takes precedence over %opts
        },
        must => {
            document_type => Str,
        },
        can => "*", # Deep checking will happen in the document class
    );

    if ( lc $document_type eq "custom" ) {
        check_args(
            args => \%opts,
            must => { custom_handler => InstanceOf[qw( UUIDB::Document )] },
            can  => "*",
        );
        carp "Overwriting existing custom document handler"
            if $self->document_handler->{ $document_type };

        # Associate it with ourselves, so documents it spawns can use
        # short-cut methods like $doc->save();
        my $custom_handler = $opts{custom_handler};
        $custom_handler->db( $self );
        $self->document_handler->{ $document_type } = $custom_handler;
        if ( %opts ) {
            $custom_handler->set_options( %opts );
        }
    } else {
        $self->init_document_handler( $document_type, %opts );
    }

    $self->default_document_type( $document_type );
}

sub storage_type {
    my ($self, $storage_type, %opts) = @_;

    unless (defined $storage_type) {
        return $self->has_storage ? $self->storage->type : undef;
    }

    check_args(
        args => {
            %opts,
            storage_type => $storage_type,
        },
        must => { storage_type => Str },
        can  => "*", # Deep checking will happen in the storage class
    );

    my $storage;
    if ( lc $storage_type eq "custom" ) {
        check_args(
            args => \%opts,
            must => { custom_storage => InstanceOf[qw( UUIDB::Storage )] },
            can  => "*",
        );
        $storage = $opts{custom_storage};

        # Associate it with ourselves
        $storage->db( $self );
    } else {
        $storage = $self->init_storage( $storage_type, %opts );
    }

    carp "Overwriting storage engine, existing documents will detach"
        if  $self->has_storage
        and $storage != $self->storage; # Rare, but not worth making noise.

    $self->storage( $storage );
}

sub create {
    my ($self, $data, $type, $as_document) = @_;
    return $self->create_typed(
        $data,
        $type || $self->default_document_type,
        $as_document,
    );
}

sub create_document {
    my ($self, $data, $type) = @_;
    return $self->create_typed(
        $data,
        $type || $self->default_document_type,
        1,
    );
}

sub create_typed {
    my ($self, $data, $type, $as_document) = @_;
    my $uuid = $self->uuid;
    my $document = $self->set_typed(
        $uuid => $data,
        type  => $type,
    );
    return $as_document ? $document : $uuid;
}

sub get {
    my ($self, $key) = @_;
    return $self->get_typed( $key, $self->default_document_type );
}

sub get_typed {
    my ($self, $key, $document_type, $as_document) = @_;

    $self->init_check;
    check_args(
        args => {
            key           => $key,
            document_type => $document_type,
            as_document   => $as_document,
        },
        must => {
            key           =>  Any, # TODO: allow Str OR ArrayRef
            document_type => [
                Str,
                qr/[^\s]/,
                sub {
                    croak "Document handler not initialized for $_[0]"
                        unless $self->document_handler->{ $_[0] };
                },
            ],
            as_document   => Bool,
        },
    );

    my $document = $self->storage->get_document(
        $key, $self->document_handler->{ $document_type }
    );
    # TODO:refactor for get_multi access ($key isa ArrayRef)
    return (
        $as_document
        ? $document # They want the document?  They got it!
        : (
            $document # Otherwise, give'em data if we have it.
            ? $document->data
            : ()
        )
    );
}

# I don't think there's a good way to pass options to this one AND the list of
# keys, not without being a bit too convoluted.  Now then, should it return the
# simple list, the documents themselves, or a hash of key => value pairs, so
# it's easy to match it all up?
sub get_multi {
    # TODO: need to figure out a good invocation signature for this.
    # ...and for graph storage, we'll want to introduce a "get_threaded" with a
    # depth setting, but that's likely to be in the Informcom specific data
    # model rather than here.
}

sub get_document {
    my ($self, $key, $type) = @_;
    # TODO: check_args

    $self->init_check();
    # Last bool for "as document"
    $self->get_typed( $key, ($type || $self->default_document_type), 1 );
}

sub save {
    my ($self, $document, %opts) = @_;
    # TODO: check_args
    $self->init_check();
    $document->uuid( $self->uuid() ) unless $document->uuid();
    return $self->storage->store_document(
        $document,
        ( $opts{storage_opts} ?
            %{ $opts{storage_opts} }
        : () ),
    );
}

sub set {
    my ($self, $key, $data, %opts) = @_;
    # TODO:check_args
    $opts{type} ||= $self->default_document_type;
    return $self->set_typed(
        $key => $data,
        %opts,
    );
}

sub set_typed {
    my ($self, $key, $value, %opts) = @_;

    $self->init_check;

    # TODO: check_args
    croak "Missing document type" unless $opts{type};

    croak "Unknown document type '$opts{type}'"
        unless $self->document_handler->{ $opts{type} };

    my $document = (
        blessed $value 
        && $value->isa( "UUIDB::Document" )
        ? $value
        : $self->document_handler->{
            $opts{type}
        }->new_from_data( $value )
    );

    $document->uuid( $key );

    # While it's possible to pass just-in-time args to the storage engine, it's
    # probably best to leave this to settings with which it was instantiated in
    # the first place.
    return $self->save(
        $document,
        ( $opts{storage_opts} ?
            %{ $opts{storage_opts} }
        : () ),
    );
}

sub exists {
    my ($self, $key) = @_;
    $self->init_check;
    return $self->storage->exists( $key );
}

# TODO: standardize on positional vs named?  Are there "options" we want to
# potentially pass during exists / delete checks?
sub delete {
    my ($self, $key, $warnings) = @_;
    return $self->storage->delete_document( $key, $warnings );
}

sub init_check {
    my ($self) = @_;

    croak "Storage not initialized"
        unless $self->storage;
}

sub uuid {
    my ($self) = @_;
    # TODO: Get a UUID from the defined provider and hand it back.
    return $self->uuid_generator->();
}

sub default_document_handler {
    my ($self) = @_;
    my $type = $self->default_document_type;
    croak "Default document type not set"
        unless $type;

    my $handler = $self->document_handler->{ $self->default_document_type };
    croak "Default document handler not initialized"
        unless $handler;

    return $handler;
}

sub init_document_handler {
    my ($self, $document_type, %opts) = @_;
    check_args(
        args => {
            document_type => $document_type,
        },
        must => {
            document_type => Str,
        },
        can => "*",
    );

    $self->document_handler->{ $document_type } ||= do {
        my $class = "UUIDB::Document::$document_type";
        if ( $document_type =~ m/::/ ) {
            # They gave us a class name, use that in its entirety.
            $class = $document_type;
        }
        safe_require $class;
        $class->new( %opts, db => $self );
    };
    return $self->document_handler->{ $document_type }
}

sub init_storage {
    my ($self, $storage_type, %opts) = @_;

    check_args(
        args => {
            %opts,
            storage_type => $storage_type,
        },
        must => { storage_type => Str },
        can  => "*",
    );

    my $class = "UUIDB::Storage::$storage_type";
    if ( $storage_type =~ m/::/ ) {
        $class = $storage_type;
    }
    safe_require $class unless is_loaded $class;
    return $class->new( %opts, db => $self  );
}

1;

=head1 AUTHOR

=cut
