package UUIDB::Util;

use v5.10;
use strict;
use warnings;

use Carp         qw( carp croak confess );
use Scalar::Util qw( blessed            );
use base         qw( Exporter           );

our @EXPORT_OK = qw(
    check_args
    is_loaded
    safe_require
);

# TODO: POD
# TODO: make it possible to pass an arrayref of constraints, all of which must
# be satisfied (where "regex" or "coderef" are also valid types)
sub check_args (%) {
    my (%arg_details) = @_;
    my $args = delete $arg_details{args};
    croak "Missing args to check"
        unless $args;

    croak "Args must be a hashref, not " . ref $args
        unless ref( $args )
        and    ref( $args ) eq 'HASH';

    my $validate = sub {
        my ($data, $validation) = @_;

        my @checks = (
            ref $validation && ref $validation eq "ARRAY"
            ?  @$validation
            : ( $validation )
        );

        foreach my $check ( @checks ) {
            if ( blessed $check ) {
                if ( ref $check eq "Regexp" ) {
                    my $pass = eval { $data =~ $check };
                    confess "Data failed regex $check: " . ( $data // "undef" )
                        unless $pass;
                } else {
                    croak "Expected Type::Tiny, not " . ref( $check )
                        unless blessed $check
                        and (
                               $check->isa( "Type::Tiny"   )
                            or $check->can( "assert_valid" )
                        );
                    my $checked = $check->validate( $data );
                    confess $checked if defined $checked;
                }
            } elsif ( ref $check eq "CODE" ) {
                local $@;
                my $pass = eval { $check->( $data ) };
                confess $@ if $@;
                confess "Data failed sub check" unless $pass;
            } else {
                confess "Invalid validator";
            }
        }
    };

    my %known_elements = map { $_ => 1 } keys %$args;
    if (my $must = delete $arg_details{must}) {
        MUST: foreach my $key ( keys %$must ) {

            delete $known_elements{$key}
                if exists $known_elements{$key};

            confess "Missing required $key"
                unless exists $args->{$key};

            $validate->( $args->{$key}, $must->{$key} );
        }
    }
    if (my $should = delete $arg_details{should}) {
        SHOULD: foreach my $key ( keys %$should ) {

            delete $known_elements{$key}
                if exists $known_elements{$key};

            unless ( exists( $args->{$key} ) ) {
                warn "Should have passed a $key but didn't, skipping";
                next SHOULD;
            }

            unless ( defined( $args->{$key} ) ) {
                warn "Skipping undefined $key";
                next SHOULD;
            }

            $validate->( $args->{$key}, $should->{$key} );
        }
    }
    if (my $can = delete $arg_details{can}) {
        return 1 if ( !ref( $can ) && $can eq "*" );
        CAN: foreach my $key ( keys %$can ) {

            delete $known_elements{$key}
                if exists $known_elements{$key};

            next CAN unless defined( $args->{$key} );
            $validate->( $args->{$key}, $can->{$key} );
        }
    }
    if ( scalar( keys( %arg_details ) ) ) {
        croak "Found unrecognized validation spec in call to check_args";
    }
    if ( scalar( keys( %known_elements ) ) ) {
        croak "Found unrecognized args in call to check_args";
    }
    return 1;
}

sub is_loaded (_) {
    my ($package_name) = @_;
    $package_name =~ s{::}{/}g;
    $package_name =~ s{(?<!\.pm)\Z}{.pm};
    return exists $INC{ $package_name };
}

sub safe_require (_) {
    my ($package_name) = @_;
    $package_name =~ s{::}{/}g;
    $package_name =~ s{(?<!\.pm)\Z}{.pm};
    require $package_name;
}

1;

