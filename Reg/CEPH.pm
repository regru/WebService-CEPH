package Reg::CEPH;

use strict;
use warnings;
use Carp;
use Reg::CEPH::NetAmazonS3;

=encoding utf8

=head1 CEPH

Клинт для CEPH, без низкоуровневого кода для общения с библиотекой Amazon S3
(она вынесена в отдельный класс)

=cut

use constant MINIMAL_MULTIPART_PART => 5*1024*1024;

=head2 new

Конструктор.

Обязательные параметры:

protocol - http/https
host - хост бэкэнда
bucket - имя бакета
key - ключ для входа
secret - secret для входа

Необязательные параметры:

driver_name - в данный момент только 'NetAmazonS3'
multipart_threshold - после какого размера файла начинать multipart upload

=cut

sub new {
    my ($class, %args) = @_;
    
    my $self = bless +{}, $class;
    
    # mandatory
    $self->{$_} = delete $args{$_} // confess "Missing $_" for (qw/protocol host bucket key secret/);
    # optional
    $self->{$_} = delete $args{$_} for (qw/driver_name multipart_threshold multisegment_threshold/);
    
    confess "Unused arguments: @{[ %args]}" if %args;
    
    $self->{driver_name} ||= "NetAmazonS3";
    $self->{multipart_threshold} ||= MINIMAL_MULTIPART_PART;
    $self->{multisegment_threshold}  ||= MINIMAL_MULTIPART_PART;
            
    confess "multipart_threshold should be greater or eq. MINIMAL_MULTIPART_PART (5Mb) (now multipart_threshold=$self->{multipart_threshold}"
        if $self->{multipart_threshold} < MINIMAL_MULTIPART_PART;
    
    my $driver_class = __PACKAGE__."::".$self->{driver_name}; # should be loaded via "use" at top of file
    $self->{driver} = $driver_class->new(map { $_ => $self->{$_} } qw/protocol host bucket key secret/ );
    
    $self;
}




=head2 upload

Загружает файл в CEPH. Если файл уже существует - он заменяется.
Если данные больше определённого размера, происходим multipart upload
Ничего не возвращает

Параметры:
0-й - $self
1-й - имя ключа
2-й - скаляр, данные ключа


=cut

sub upload {
    my ($self, $key) = (shift, shift);

    if (length($_[0]) > $self->{multipart_threshold}) {
        
        my $multipart = $self->{driver}->initiate_multipart_upload($key);
        
        my $len = length($_[0]);
        my $offset = 0;
        my $part = 0;
        while ($offset < $len) {
            my $chunk = substr($_[0], $offset, $self->{multipart_threshold});
            
            $self->{driver}->upload_part($multipart, ++$part, $chunk);
            
            $offset += $self->{multipart_threshold};
        }
        $self->{driver}->complete_multipart_upload($multipart);
    }
    else {
        $self->{driver}->upload_single_request($key, $_[0]);
    }
 
    return;   
}

=head2 download

Скачивает данные с именем $key и возвращает их.
Если ключ не существует, возвращает undef.

=cut

sub download {
    my ($self, $key) = @_;
    
    my $offset = 0;
    my $data;
    while() {
        my ($dataref, $bytesleft) = $self->{driver}->download_with_range($key, $offset, $offset + $self->{multisegment_threshold});
        unless ($dataref) {
            $offset ? confess : return;
        }
        $offset += length($$dataref);
        $data .= $$dataref;
        last unless $bytesleft;
    };
    $data;
}

=head2 size

Возвращает данные размер ключа с именем $key в байтах,
если ключ не существует, возвращает undef

=cut

sub size {
    my ($self, $key) = @_;
    
    $self->{driver}->size($key); 
}

=head2 delete

Удаляет ключ с именем $key, ничего не возвращает. Если ключ
не существует, не выдаёт ошибку

=cut

sub delete {
    my ($self, $key) = @_;
    
    $self->{driver}->delete($key); 
}

1;
