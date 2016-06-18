package WebService::CEPH::FileShadow;

# VERSION

use strict;
use warnings;
use Carp;
use Fcntl qw/:seek/;
use File::Copy;
use File::Slurp qw/read_file/;
use parent qw( WebService::CEPH );

=head2 upload

Перекрытый метод WebService::CEPH

=cut

sub new {
    my ($class, %options) = @_;
    my %new_options;

    $new_options{$_} = delete $options{$_} for (qw/fs_shadow_path mode/);
    my $self = $class->SUPER::new(%options);

    $self->{$_} = $new_options{$_} for keys %new_options;

    $self;
}

sub _filepath {
    my ($self, $key) = @_;
    confess if $key =~ m!\.\./!;
    confess if $key =~ m!/\.\.!;
    $self->{fs_shadow_path}.$self->{bucket}."/".$key;
}
sub upload {
    my ($self, $key) = (shift, shift);

    if ($self->{mode} =~ /s3/) {
        $self->SUPER::upload($key, $_[0]);
    }
    if ($self->{mode} =~ /fs/) {
        my $path = $self->_filepath($key);
        open my $f, ">", $path or confess;
        binmode $f;
        print $f $_[0] or confess;
        close $f or confess;
    }
}

sub upload_from_file {
    my ($self, $key, $fh_or_filename) = @_;

    if ($self->{mode} =~ /s3/) {
        $self->SUPER::upload_from_file($key, $fh_or_filename);
    }
    if ($self->{mode} =~ /fs/) {
        my $path = $self->_filepath($key);
        seek($fh_or_filename, 0, SEEK_SET) if (ref $fh_or_filename);
        copy($fh_or_filename, $path);
    }
}

sub download {
    my ($self, $key) = @_;

    if ($self->{mode} =~ /s3/) {
        return $self->SUPER::download($key);
    }
    elsif ($self->{mode} =~ /fs/) {
        read_file( $self->_filepath($key), binmode => ':raw' )
    }
}

sub download_to_file {
    my ($self, $key, $fh_or_filename) = @_;

    if ($self->{mode} =~ /s3/) {
        $self->SUPER::download_to_file($key, $fh_or_filename);
    }
    elsif ($self->{mode} =~ /fs/) {
        copy( $self->_filepath($key), $fh_or_filename );
    }
}

sub size {
    my ($self, $key) = @_;

    if ($self->{mode} =~ /s3/) {
        return $self->SUPER::size($key);
    }
    elsif ($self->{mode} =~ /fs/) {
        return -s $self->_filepath($key);
    }
}

sub delete {
    my ($self, $key) = @_;

    if ($self->{mode} =~ /s3/) {
        $self->SUPER::delete($key);
    }
    elsif ($self->{mode} =~ /fs/) {
        unlink($self->_filepath($key));
    }
}

sub query_string_authentication_uri {
    my ($self, $key, $expires) = @_;

    if ($self->{mode} =~ /s3/) {
        $self->SUPER::query_string_authentication_uri($key, $expires);
    }
    else {
        confess;
    }
}

1;
