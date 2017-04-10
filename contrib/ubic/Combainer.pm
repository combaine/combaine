package Ubic::Service::Combainer;

# ABSTRACT: run cocaine as Ubic service

=head1 SYNOPSIS

    # /etc/ubic/service/combainer.ini:
        module = Ubic::Service::Combainer
        [options]
        user = cocaine
        rlimit_nofile = 65535
        rlimit_core = -1

=head1 DESCRIPTION

Daemon's user is always B<cocaine>.

=cut

use strict;
use warnings;

use Ubic::Daemon qw(:all);
use Ubic::Result qw(result);
use parent qw(Ubic::Service::SimpleDaemon);
use Params::Validate qw(:all);
use Ubic::Service::Shared::Dirs;

my %opt2arg = ();
for my $arg (qw(rlimit_nofile rlimit_core))
{
    my $opt = $arg;
    $opt2arg{$opt} = $arg;
}

sub new {
    my $class = shift;

    my $params = validate(@_, {
        rlimit_nofile => { type => SCALAR,
                           regex => qr/^\d+$/,
                           optional => 1,
                        },
        rlimit_core => {   type => SCALAR,
                           regex => qr/^\-?\d+$/,
                           optional => 1 },
    });

    my $ulimits;
    if (defined $params->{rlimit_nofile}) { $ulimits->{"RLIMIT_NOFILE"} = $params->{rlimit_nofile}; undef $params->{rlimit_nofile};  };
    if (defined $params->{rlimit_core})   { $ulimits->{"RLIMIT_CORE"} = $params->{rlimit_core}; undef $params->{rlimit_core}; };

    my $bin = [
        "/usr/bin/combainer -loglevel=DEBUG -logoutput='/var/log/cocaine-core/combainer.log'",
    ];

    return $class->SUPER::new({
        bin => $bin,
        user => 'root',
        ulimit => $ulimits || {},
        daemon_user => 'cocaine',
        ubic_log => '/var/log/ubic/combainer/ubic.log',
        stdout => "/var/log/ubic/combainer/stdout.log",
        stderr => "/var/log/ubic/combainer/stderr.log",
    });
}

sub start_impl {
    my $self = shift;

    Ubic::Service::Shared::Dirs::directory_checker("/var/run/combaine", $self->{"daemon_user"} );
    Ubic::Service::Shared::Dirs::directory_checker("/var/spool/combainer", $self->{"daemon_user"} );
    Ubic::Service::Shared::Dirs::directory_checker("/var/log/cocaine-core", $self->{"daemon_user"} );
    Ubic::Service::Shared::Dirs::directory_checker("/var/log/ubic/combainer", $self->{"daemon_user"} );
    $self->SUPER::start_impl(@_);
}

sub reload {
    my ( $self ) = @_;
    my $status = check_daemon($self->pidfile) or die result('not running');
    kill HUP => $status->pid;

    return 'reloaded';
}

1;
