=encoding utf8

=head1 CEPH

Клинт для CEPH, без низкоуровневого кода для общения с библиотекой Amazon S3
(она вынесена в отдельный класс)

=cut

package Reg::CEPH;

use strict;
use warnings;
use Carp;
use Reg::CEPH::NetAmazonS3;
use Digest::MD5 qw/md5_hex/;

use constant MINIMAL_MULTIPART_PART => 5*1024*1024;

sub _check_ascii_key { confess "Key should be ASCII-only" unless $_[0] !~ /[^\x00-\x7f]/ }

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
    
    _check_ascii_key($key);

    if (length($_[0]) > $self->{multipart_threshold}) {
        
        my $multipart = $self->{driver}->initiate_multipart_upload($key, md5_hex($_[0]));
        
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

Скачивает данные объекта с именем $key и возвращает их.
Если объект не существует, возвращает undef.

Если размер объекта по факту окажется больше multisegment_threshold,
объект будет скачан несколькими запросами с заголовком Range (т.е. multi segment download).

=cut

sub download {
    my ($self, $key) = @_;
    
    _check_ascii_key($key);
    
    my $offset = 0;
    my $data;
    my $check_md5 = undef;
    my $md5 =  Digest::MD5->new;
    while() {
        my ($dataref, $bytesleft, $etag, $custom_md5) = $self->{driver}->download_with_range($key, $offset, $offset + $self->{multisegment_threshold});

        # Если объект не найден - возвращаем undef
        # даже если при мультисегментном скачивании объект неожиданно исчез на каком-то сегменте, значит
        # его кто-то удалил, нужно всё же вернуть undef
        return unless ($dataref);

        # Проверяем md5 только если ETag "нормальный" с md5 (был не multipart upload)
        if (!defined $check_md5) {
            my ($etag_md5) = $etag =~ /^([0-9a-f]+)$/;
            
            #print "ETAG $etag_md5 CUSTOM $custom_md5\n";
            confess "ETag looks like valid md5 and x-amz-meta-md5 presents but they do not match"
                if ($etag_md5 && $custom_md5 && $etag_md5 ne $custom_md5);
            if ($etag_md5) {
                $check_md5 = $etag_md5;
            } elsif ($custom_md5) {
                $check_md5 = $custom_md5;
            } else {
                $check_md5 = 0;
            }
        }
        if ($check_md5) {
            $md5->add($$dataref);
        }
        
        $offset += length($$dataref);
        $data .= $$dataref;
        last unless $bytesleft;
    };
    if ($check_md5) {
        my $got_md5 = $md5->hexdigest;
        confess "MD5 missmatch, got $got_md5, expected $check_md5" unless $got_md5 eq $check_md5;
    }
    $data;
}

=head2 size

Возвращает размер объекта с именем $key в байтах,
если ключ не существует, возвращает undef

=cut

sub size {
    my ($self, $key) = @_;
    
    _check_ascii_key($key);
    
    $self->{driver}->size($key); 
}

=head2 delete

Удаляет объект с именем $key, ничего не возвращает. Если объект
не существует, не выдаёт ошибку

=cut

sub delete {
    my ($self, $key) = @_;
    
    _check_ascii_key($key);
    
    $self->{driver}->delete($key); 
}

1;
