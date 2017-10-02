#!/usr/bin/env perl

use strict;
use warnings;

use Test::Most;
use Types::Standard qw( Any ArrayRef Bool Str );
use UUIDB::Util qw(
    check_args
);

# TODO: docs

throws_ok {
    check_args()
}   qr/Missing args to check/,
    "Dies on missing 'args'";

throws_ok {
    check_args(
        args => [],
    );
}   qr/Args must be a hashref/,
    "Dies on wrong 'args' type";

throws_ok {
    check_args(
        args => {
            frob => "knob",
        },
        must => {
            frob => "this is not a validator",
        }
    );
}   qr/Invalid validator/,
    "Dies on invalid validator";

throws_ok {
    check_args(
        args => {},
        must => {
            frob => Any,
        },
    )
}   qr/Missing required frob/,
    "Dies on missing required arg";


throws_ok {
    check_args(
        args => {
            frob => undef,
        },
        must => {
            frob => Str,
        },
    )
}   qr/did not pass type constraint/,
    "Dies on undef required arg";

lives_ok {
    check_args(
        args => {
            frob => "I, sir, am a string.",
        },
        must => {
            frob => Str,
        }
    );
}   "Satisfied required arg passes";

warning_like {
    check_args(
        args   => {},
        should => {
            frob => Any,
        }
    );
}   qr/Should have passed a frob but didn't/,
    "Warns on missing 'should' arg";

warning_like {
    check_args(
        args   => {
            frob => undef,
        },
        should => {
            frob => Any,
        }
    );
}   qr/Skipping undefined frob/,
    "Warns on undef 'should' arg";


throws_ok {
    check_args(
        args => {
            frob => {},
        },
        should => {
            frob => Str,
        },
    )
}   qr/did not pass type constraint/,
    "Dies on wrong type 'should' arg";

lives_ok {
    check_args(
        args => {
            frob => "And I, madame, am a string also.",
        },
        should => {
            frob => Str,
        },
    )
}   "Satisfied 'should' arg passes";

lives_ok {
    check_args(
        args => {
            frob => "Eyup",
        },
        can => {
            frob => Any,
            knob => Any,
        },
    );
}   "Satisfied 'can' arg(s) pass";

throws_ok {
    check_args(
        args => {
            frob => "knob",
        },
        what => {
            ever => "man",
        },
    );
}   qr/Found unrecognized validation spec in call to check_args/,
    "Dies on unknown validation spec";

throws_ok {
    check_args(
        args => {
            frob => "knob",
        },
    );
}   qr/Found unrecognized args in call to check_args/,
    "Dies on unknown args";

throws_ok {
    check_args(
        args => {
            frob => "knob",
        },
        must => {
            frob => qr/bonk/,
        },
    );
}   qr/Data failed regex/,
    "Dies on regex validation fail";

throws_ok {
    check_args(
        args => {
            frob => "knob",
        },
        must => {
            frob => sub { $_[0] ne "knob" },
        },
    );
}   qr/.*/,
    "Dies on code validation fail";

lives_ok {
    check_args(
        args => {
            frob => "bob",
        },
        must => {
            frob => [
                Str,
                qr/.o./,
                sub { defined $_[0] },
            ],
        },
    );
}   "Stackable checks";

done_testing;
