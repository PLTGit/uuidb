package UUIDB::Document;

use v5.10;
use strict;
use warnings;

use Moo;
use Carp            qw( carp croak );
use Scalar::Util    qw( blessed    );
use Types::Standard qw(
    Any  ArrayRef Bool
    HashRef InstanceOf
    Maybe   Ref    Str
);
use UUIDB::Util     qw( check_args );

# TODO: POD, tests
has db => (
    is => "rw",
    isa => InstanceOf[qw( UUIDB )],
);

has data => (
    is => "rw",
    isa => Any,
);

# Free-form hash for information *about* the document, but which is not stored
# along with it; things like timestamps, or storage advice (as used by
# L<UUIDB::Storage> members).
has meta => (
    is      => "rw",
    isa     => HashRef,
    default => sub { {} },
);

# TODO: is_uuid_string as a constraint here.
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

has fatal_unknown_extracts => (
    is      => "rw",
    isa     => Bool,
    default => sub { 0 },
);

sub BUILD {
    my ($self, $opts) = @_;
    # Remove any of those settings which are attribute specific.
    # Pass the remainder onto options.
    $self->set_options( %$opts );
}

sub set_options {
    my ($self, %opts) = @_;
    # TODO: document_options
}

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

# Given a field specification, return a corresponding value from our data.
# Assumes the internal data representation is a hash.
# TODO: See if there's some equivalent for DeepHash navigation out there? Or
# maybe just rewrite it?  E.g., be able to do this.is.some.key to retrieve a
# nested value from "{ this => { is => { some => { key => $value } } } }"
sub extract {
    my ($self, @fields) = @_;

    return undef unless $self->data;

    check_args(
        args => {
            field => \@fields,
        },
        must => {
            field => ArrayRef[Str],
        },
    );

    my %return;
    foreach my $field ( @fields ) {
        # Is our data an object of sorts, with a named accessor for the field?
        if (
            blessed $self->data
        ) {
            if ( $self->data->can( $field ) ) {
                $return{ $field } = $self->data->$field();
            } elsif ( $self->fatal_unknown_extracts ) {
                croak "Don't know how to extract field from object";
            }
        } elsif ( ref( $self->data ) eq 'HASH' ) {
            $return{ $field } = $self->data->{ $field };
        } else {
            croak "Don't know how to extract document data";
        }
    }

    return \%return;
}

# This is the frozen version of our own data (if we're in instance mode)
sub frozen {
    my ($self) = @_;
    return $self->freeze( $self->data );
}

sub new_from_data {
    my ($self, $data) = @_;
    my $class = blessed $self;

    # TODO: We're making a new stateful document, but we want our options to
    # propagate to it.  We need a quick way of consolidating those options
    # for propagation.  Doing this with a minimum overhead in memory and
    # performance would also be nice; especially since handing around a lot of
    # refs is a *great* way to cause memory leaks.
    return $class->new(
        db   => $self->db,
        data => $data,
        propagate_uuid => $self->propagate_uuid,
    );
}

sub save {
    my ($self) = @_;
    return $self->db->save( $self );
}

# Oftimes used in the naming of data entries in storage, during transmission,
# etc.
sub suffix {
    my ($self) = @_;
    if ( my $type = $self->type ) {
        return lc $type;
    }
    return undef;
}

sub update {
    my ($self) = @_;
    carp "Document does not yet have a UUID, a new one will be assigned"
        unless $self->uuid();
    return $self->save();
}

1;
