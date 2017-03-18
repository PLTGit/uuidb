package UUIDB::Document;

use v5.10;
use strict;
use warnings;

use Moo;
use Carp qw( croak );
use Types::Standard qw( Any InstanceOf Maybe Str );
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

has id => (
    is  => "rw",
    isa => Maybe[Str],
);

# TODO: specific die tests for this
before [qw( data id )] => sub {
    my ($self, $id) = @_;
    croak "The base UUIDB::Document class cannot be used as a document instance"
        if blessed $self eq __PACKAGE__;
};

sub type   ($ ) { croak "The 'type' method must be overridden in descendant classes"   }
sub freeze ($$) { croak "The 'freeze' method must be overridden in descendant classes" }
sub thaw   ($$) { croak "The 'thaw' method must be overridden in descendant classes"   }

# This is the frozen version of our own data (if we're in instance mode)
sub frozen ($) {
    my ($self) = @_;
    return $self->freeze( $self->data );
}

sub new_from_data ($$) {
    my ($self, $data) = @_;
    my $class = blessed $self;

    return $class->new(
        db   => $self->db,
        data => $data,
    );
}

sub save ($) {
    my ($self) = @_;
    return $self->db->save( $self );
}

1;
