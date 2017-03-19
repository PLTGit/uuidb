package UUIDB;

use v5.10;
use strict;
use warnings;

=head1 NAME

=head1 VERSION

=head1 SYNOPSIS

    my $uuidb = UUIDB->new(
        name          => "Test",
        document_type => "JSON",
        storage_type  => "Memory",
    );

    # JSON only because that's what we're demonstrating; could be anything.
    my %data = (
        any => "kind",
        of  => "JSON",
        serializable => [qw(
            data
        )],
    );

    # Create it
    my $key = $db->create( \%data );

    # Read it
    my %data_copy = %{ $db->get( $key ) };

    # Update it
    $data_copy{change} = "something";
    $db->set( $key, \%data_copy );

    # Delete it
    $db->delete( $key );

    # Double check.
    say "Nope" unless $db->exists( $key );

    # Slightly more OO version
    # Create it
    my $document = $db->create_document( \%data );
    $key = $document->uuid();
    $document->data->{still} = "simple data";
    $document->save(); # or $document->update();

    if (my $document_copy = $db->get_document( $key )) {
        is_deeply(
            $document_copy->data,
            $document->data,
            "Everything matches."
        );
    }

=head1 DESCRIPTION

=cut

use Carp qw( carp croak );
use Scalar::Util qw( blessed );
use Types::Standard qw( HashRef InstanceOf Map Ref Str );
use UUID::Tiny qw( create_uuid_as_string is_uuid_string UUID_V4 );
use UUIDB::Util qw( check_args is_loaded safe_require );

# Makes some stuff easier.
use Moo;

has name => (
    is  => 'ro',
    isa => Str,
);

# TODO: validation
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
    is  => 'rw',
    isa => InstanceOf[qw( UUIDB::Storage )],
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


# TODO: BUILDARGS validation

sub BUILD ($$) {
    my ($self, $args) = @_;
    if ( $args->{document_type} ) {
        $self->document_type( $args->{document_type} );
    }
    # TODO: document_options.
    if ( $args->{storage_type} ) {
        $self->storage_type( $args->{storage_type} );
    }
    # TODO: storage_options, which should include something like "set uuid on
    # the stored document's *data* object, so it keeps that around as well.  Or
    # maybe that should be an option in the document handler, so that if
    # configured, the ->uuiid() will have an "after" modifier which propagates
    # to the data itself (if it knows how to do that, which can be any manner of
    # callback).  Which reminds me, we really ought to introduce an event model
    # here generally.
}

sub document_type ($$;$)  {
    my ($self, $document_type, $custom_handler) = @_;
    # TODO: check_args
    check_args(
        args => {
            document_type => $document_type,
        },
        must => {
            document_type => Str,
        },
        can => {
            custom_handler => InstanceOf[qw( UUIDB::Document )],
        }
    );

    my $handler;
    if ( lc $document_type eq "custom" ) {
        croak "Missing custom document handler"
            unless $custom_handler;
        carp "Overwriting existing custom document handler"
            if $self->document_handler->{ $document_type };
        $handler = $custom_handler;

        # Associate it with ourselves, so that documents it spawns can use
        # short-cut methods like $doc->save();
        $handler->db( $self );
    } elsif ( !$self->document_handler->{ $document_type } ) {
        my $class = "UUIDB::Document::$document_type";
        if ( $document_type =~ m/::/ ) {
            # They gave us a class name, use that in its entirety.
            $class = $document_type;
        }
        safe_require $class;
        $handler = $class->new( db => $self );
    }

    $self->default_document_type( $document_type );

    # Only set if we already passed proper type constraints andchecks above.
    if ( $handler ) {
        $self->document_handler->{ $document_type } = $handler;
    }
}

sub storage_type ($$;$)  {
    my ($self, $storage_type, $custom_storage) = @_;
    # TODO: check_args
    check_args(
        args => {
            storage_type => $storage_type,
        },
        must => {
            storage_type => Str,
        },
        can => {
            custom_storage => InstanceOf[qw( UUIDB::Storage )],
        }
    );

    my $storage;
    my $storage_type_key = lc $storage_type;
    if ( lc $storage_type eq "custom" ) {
        croak "Missing custom storage handler"
            unless $custom_storage;

        $storage = $custom_storage;

        # Associate it with ourselves
        $storage->db( $self );
    } else {
        my $class = "UUIDB::Storage::$storage_type";
        if ( $storage_type =~ m/::/ ) {
            $class = $storage_type;
        }
        safe_require $class;
        $storage = $class->new( db => $self );
    }

    carp "Overwriting storage engine, existing documents will detach"
        if $self->storage;

    $self->storage( $storage );
}

sub create ($$;$$) {
    my ($self, $data, $type, $as_document) = @_;
    return $self->create_typed(
        $data,
        $type || $self->default_document_type,
        $as_document,
    );
}
sub create_document ($$;$) {
    my ($self, $data, $type) = @_;
    return $self->create_typed(
        $self,
        $data,
        $type || $self->default_document_type,
        1,
    );
}
sub create_typed ($$$;$) {
    my ($self, $data, $type, $as_document) = @_;
    my $uuid = $self->uuid;
    my $document = $self->set_typed(
        $uuid => $data,
        type  => $type,
    );
    return $as_document ? $document : $uuid;
}

sub get ($$) {
    my ($self, $key) = @_;
    return $self->get_typed( $key, $self->default_document_type );
}
sub get_typed ($$$;$) {
    my ($self, $key, $type, $as_document) = @_;

    # TODO: check_args

    # TODO: Encapsulate init chekcks
    $self->init_check;

    croak "Document handler not initialized for $type"
        unless $self->document_handler->{ $type };

    # TODO: should probably pass the handler to the storage engine to coordinate,
    # rather than calling thaw ourselves.
    my $document = $self->storage->get_document(
        $key, $self->document_handler->{ $type }
    );
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
sub get_multi ($@) {
    # TODO: need to figure out a good invocation signature for this.
    # ...and for graph storage, we'll want to introduce a "get_threaded" with a
    # depth setting, but that's likely to be in the Informcom specific data
    # model rather than here.
}

sub get_document ($$;$) {
    my ($self, $key, $type) = @_;
    # TODO: check_args

    $self->init_check();
    # Last bool fro "as document"
    $self->get_typed( $key, ($type || $self->default_document_type), 1 );
}

sub save ($$;%) {
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

sub set ($$$;%) {
    my ($self, $key, $data, %opts) = @_;
    # TODO:check_args
    $opts{type} ||= $self->default_document_type;
    return $self->set_typed(
        $key => $data,
        %opts,
    );
}
sub set_typed ($$$%) {
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

sub exists ($$) {
    my ($self, $key) = @_;
    $self->init_check;
    return $self->storage->exists( $key );
}

# TODO: standardize on positional vs named?  Are there "options" we want to
# potentially pass during exists / delete checks?
sub delete ($$;$) {
    my ($self, $key, $warnings) = @_;
    return $self->storage->delete( $key, $warnings );
}

sub init_check ($) {
    my ($self) = @_;

    croak "Storage not initialized"
        unless $self->storage;
}

sub uuid ($) {
    my ($self) = @_;
    # TODO: Get a UUID from the defined provider and hand it back.
    return $self->uuid_generator->();
}


1;

=head1 AUTHOR

=cut
