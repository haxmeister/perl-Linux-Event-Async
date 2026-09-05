use v5.36;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME          => 'Linux::Event::Async',
    VERSION_FROM  => 'lib/Linux/Event/Async.pm',
    ABSTRACT_FROM => 'lib/Linux/Event/Async.pm',
    AUTHOR        => 'Joshua S. Day',
    LICENSE       => 'perl_5',
    MIN_PERL_VERSION => '5.36.0',
    DIR => ['xsfuture'],
    PREREQ_PM => {
        'Future::AsyncAwait' => '0.71',
        'Linux::Event'       => '0.110',
    },
    TEST_REQUIRES => {
        'Test::More' => 0,
    },
    XS => {
        'Async.xs' => 'Async.c',
    },
    clean => {
        FILES => 'Async.c',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
        resources => {
            repository => {
                type => 'git',
                url  => 'https://github.com/haxmeister/perl-Linux-Event-Async.git',
                web  => 'https://github.com/haxmeister/perl-Linux-Event-Async',
            },
        },
    },
);
