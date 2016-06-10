package Reg::CEPH::NetAmazonS3;

use strict;
use warnings;
use Carp;
use Net::Amazon::S3;
use HTTP::Status;
use Digest::MD5 qw/md5_hex/;

=encoding utf8

=cut

=head2 new

=cut

sub new {
    my ($class, %args) = @_;
    
    my $self = bless +{}, $class;
    
    $self->{$_} = delete $args{$_} // confess "Missing $_" for (qw/protocol host bucket key secret/);
    confess "Unused arguments %args" if %args;
    
    
    my $s3 = Net::Amazon::S3->new({
        aws_access_key_id     => $self->{key},
        aws_secret_access_key => $self->{secret}, # TODO: фильтровать в логировании?
        host                  => $self->{host},
        retry                 => 1,
    });
    
    $self->{client} =  Net::Amazon::S3::Client->new( s3 => $s3 );
    $self;
}


sub _request_object {
    my ($self) = @_;
    
    $self->{client}->bucket(name => $self->{bucket});
}


sub upload_single_request {
    my ($self, $key) = (shift, shift);

    $self->_request_object->object( key => $key, acl_short => 'private' )->put($_[0]);
}

sub initiate_multipart_upload {
    my ($self, $key) = @_;
    
    my $object = $self->_request_object->object( key => $key, acl_short => 'private' );
    
    my $upload_id = $object->initiate_multipart_upload;
    
    +{ key => $key, upload_id => $upload_id, object => $object};
}

sub upload_part {
    
    #use Data::Dumper;print Dumper \@_;
    my ($self, $multipart_upload, $part_number) = (shift, shift, shift);
    
    #print "VALUE $_[0]\n";
    $multipart_upload->{object}->put_part(
        upload_id => $multipart_upload->{upload_id},
        part_number => $part_number,
        value => $_[0]
    );
    
    # TODO:Part numbers should be in accessing order (in case someone uploads in parallel) ! 
    push @{$multipart_upload->{parts} ||= [] }, $part_number;
    push @{$multipart_upload->{etags} ||= [] }, md5_hex($_[0]);
}

sub complete_multipart_upload {
    my ($self, $multipart_upload) = @_;
    
    $multipart_upload->{object}->complete_multipart_upload(
        upload_id => $multipart_upload->{upload_id},
        etags => $multipart_upload->{etags},
        part_numbers => $multipart_upload->{parts}
    );
}


sub download_with_range {
    my ($self, $key, $first, $last) = @_;
    
    
    # TODO: How and when to validate ETag here?
    my $http_request = Net::Amazon::S3::Request::GetObject->new(
        s3     => $self->{client}->s3,
        bucket => $self->{bucket},
        key    => $key,
        method => 'GET',
    )->http_request;
    
    if (defined $first) {
        $last //= '';
        $http_request->headers->header("Range", "bytes=$first-$last");
    }
    
    my $http_response = $self->{client}->_send_request_raw($http_request);
    #print $http_request->as_string, $http_response->as_string ;
    if ( $http_response->code == 404 && $http_response->decoded_content =~ m!<Code>NoSuchKey</Code>!) {
        return;
    }
    elsif (is_error($http_response->code)) {
        my ($err) = $http_response->decoded_content =~ m!<Code>(.*)</Code>!;
        $err //= 'none';
        confess "Unknown error ".$http_response->code." $err";
    } else {
        my $left = undef;
        if (defined $first) {
            my $range = $http_response->header('Content-Range') // confess;
            my ($f, $l, $total) = $range =~ m!bytes (\d+)\-(\d+)/(\d+)! or confess;
            $left = $total - ( $l + 1);
        }
        
        my $etag = $http_response->header('ETag');
        if ($etag) {
            $etag =~ s/^"//;
            $etag =~ s/"$//;
        }
        
        return (\$http_response->decoded_content, $etag, $left);
    }
}

sub size {
    my ($self, $key) = @_;
    
    my $http_request = Net::Amazon::S3::Request::GetObject->new(
        s3     => $self->{client}->s3,
        bucket => $self->{bucket},
        key    => $key,
        method => 'HEAD',
    )->http_request;
    
    my $http_response = $self->{client}->_send_request_raw($http_request);
    if ( $http_response->code == 404) { # It's not possible to distinct between NoSuchkey and NoSuchBucket??
        return undef;
    }
    elsif (is_error($http_response->code)) {
        confess "Unknown error ".$http_response->code;
    }
    else {
        return $http_response->header('Content-Length');
    }
    
    
    
}

sub delete {
    my ($self, $key) = @_;
    
    $self->_request_object->object( key => $key )->delete;
}

1;
