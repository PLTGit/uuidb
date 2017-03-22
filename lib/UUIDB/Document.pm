package UUIDB::Document;

use v5.10;
use strict;
use warnings;

use Moo;
use Carp qw( carp croak );
use Types::Standard qw( Any Bool InstanceOf Maybe Ref Str );
use Scalar::Util qw( blessed );

# TODO: POD, tests
has db => (
    is => "rw",
    isa => InstanceOf[qw( UUIDB )],
);

has data => (
    is => "rw",
    isa => Any,
);

has uuid => (
    is      => "rw",
    isa     => Maybe[Str],
    trigger => 1,
);

has propagate_uuid => (
    is      => "rw",
    isa     => Maybe[Bool, Ref[qw( CODE )]],
    default => sub { 0 },
);

# TODO: specific die tests for this
before [qw( data uuid )] => sub {
    my ($self, $x) = @_;
    croak "The base UUIDB::Document class cannot be used as a document instance"
        if blessed $self eq __PACKAGE__;
};

sub _trigger_uuid {
    my ($self, $uuid) = @_;
    if (my $propagation = $self->propagate_uuid) {
        # If it's a coderef, invoke it with $self as its arg.
        # if it's a simple bool, just assume the best and copy it straight into
        # data.
        # TODO: do this when "data" changes, too? (in case uuid is set first)
        if ( ref $propagation ) {
            $propagation->( $self );
        } else {
            # TODO: abstract this to "copy_uuid_to_data" and call it here.
            $self->data->{uuid} = $self->uuid();
        }
    }
}

sub type   { croak "The 'type' method must be overridden in descendant classes"   }
sub freeze { croak "The 'freeze' method must be overridden in descendant classes" }
sub thaw   { croak "The 'thaw' method must be overridden in descendant classes"   }

# This is the frozen version of our own data (if we're in instance mode)
sub frozen {
    my ($self) = @_;
    return $self->freeze( $self->data );
}

sub new_from_data {
    my ($self, $data) = @_;
    my $class = blessed $self;

    return $class->new(
        db   => $self->db,
        data => $data,
    );
}

sub update {
    my ($self) = @_;
    carp "Document does not yet have a UUID, a new one will be assigned"
        unless $self->uuid();
    return $self->save();
}

sub save {
    my ($self) = @_;
    return $self->db->save( $self );
}

# Given a field specification, return a corresponding value from our data.
# Assumes the internal data representation is a hash.
# TODO: See if there's some equivalent for DeepHash navigation out there? Or
# maybe just rewrite it?  E.g., be able to do this.is.some.key to retrieve a
# nested value from "{ this => { is => { some => { key => $value } } } }"
sub extract {
    my ($self, $field) = @_;
    check_args(
        args => {
            field => $field,
        },
        must => {
            field => [
                Str,
                qr/./, # Must not be zero length
            ],
        },
    );

    # Is our data an object of sots, with a named accessor for the field?
    if (
        blessed $self->data
        and     $self->data->can( $field )
    ) {
        return $self->data->$field();
    } elsif ( ref( $self->data ) eq 'HASH' ) {
        return $self->data->{ $field };
    }

    return ();
}
1;
