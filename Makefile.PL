use v5.36;
use strict;
use warnings;
use ExtUtils::MakeMaker;

if ($^O ne 'linux') {
    print "OS unsupported: Linux::Event::Async supports Linux only\n";
    exit 0;
}

my @public_modules = qw(
    Linux::Event::Async
    Linux::Event::Async::Future
    Linux::Event::Async::Stream
    Linux::Event::Async::Listener
);

my $distribution_version = MM->parse_version('lib/Linux/Event/Async.pm');
my %provides = map {
    my $file = "lib/$_.pm";
    $file =~ s{::}{/}g;
    $_ => {
        file    => $file,
        version => $distribution_version,
    };
} @public_modules;

WriteMakefile(
    NAME             => 'Linux::Event::Async',
    VERSION_FROM     => 'lib/Linux/Event/Async.pm',
    ABSTRACT_FROM    => 'lib/Linux/Event/Async.pm',
    AUTHOR           => 'Joshua S. Day <hax@cpan.org>',
    LICENSE          => 'perl_5',
    MIN_PERL_VERSION => '5.036000',
    DIR              => ['xsfuture'],
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    PREREQ_PM => {
        'Future::AsyncAwait' => '0.71',
        'Linux::Event'       => '0.110',
    },
    TEST_REQUIRES => {
        'Test::More' => 0,
        'Test2::V0'  => 0,
    },
    XS => {
        'Async.xs' => 'Async.c',
    },
    clean => {
        FILES => 'Async.c',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
        no_index => {
            package => [qw(
                Linux::Event::Async::Stream::Awaitable
                Linux::Event::Async::Listener::Awaitable
            )],
        },
        provides => \%provides,
        resources => {
            homepage => 'https://github.com/haxmeister/perl-Linux-Event-Async',
            bugtracker => {
                web => 'https://github.com/haxmeister/perl-Linux-Event-Async/issues',
            },
            repository => {
                type => 'git',
                url  => 'https://github.com/haxmeister/perl-Linux-Event-Async.git',
                web  => 'https://github.com/haxmeister/perl-Linux-Event-Async',
            },
        },
    },
);
