use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Linux::Event::Async::Future',
    VERSION_FROM     => '../lib/Linux/Event/Async/Future.pm',
    ABSTRACT         => 'Native Future state for Linux::Event::Async',
    AUTHOR           => 'Joshua S. Day',
    LICENSE          => 'perl_5',
    MIN_PERL_VERSION => '5.036000',
    PM               => {},
    XS               => { 'Future.xs' => 'Future.c' },
    OBJECT           => 'Future$(OBJ_EXT)',
    clean            => { FILES => 'Future.c' },
);
