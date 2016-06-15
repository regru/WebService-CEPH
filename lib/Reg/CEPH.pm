=encoding utf8

=head1 NAME

Reg::CEPH

=head1 DESCRIPTION

Клинт для CEPH, без низкоуровневого кода для общения с библиотекой Amazon S3
(она вынесена в отдельный класс).

Обработка ошибок (исключения их тип итп; повторы неудачных запросов) - на совести более низкоуровневой библиотеки,
если иное не гарантируется в этой документации.

=cut

package Reg::CEPH;

# VERSION

use strict;
use warnings;
use Carp;
use Reg::CEPH::NetAmazonS3;
use Digest::MD5 qw/md5_hex/;
use Fcntl qw/:seek/;

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
multipart_threshold - после какого размера файла (в байтах) начинать multipart upload
multisegment_threshold - после какого размера файла (в байтах) будет multisegment download

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
    $self->_upload($key, sub { substr($_[0], $_[1], $_[2]) }, length($_[0]), md5_hex($_[0]), $_[0]);
}

=head2 upload_from_file

То же, что upload, но происходит чтение из файла.
Параметры:
0-й - $self
1-й - имя ключа
2-й - имя файла (если скаляр), иначе открытый filehandle

Дваждый проходит по файлу, высчитывая md5. Файл не должен быть пайпом, его размер не должен меняться.

=cut

sub upload_from_file {
    my ($self, $key, $fh_or_filename) = @_;
    my $fh = do {
        if (ref $fh_or_filename) {
            seek($fh_or_filename, 0, SEEK_SET);
            $fh_or_filename
        }
        else {
            open my $f, "<", $fh_or_filename;
            binmode $f;
            $f;
        }
    };
    
    my $md5 = Digest::MD5->new;
    $md5->addfile($fh);
    seek($fh, 0, SEEK_SET);
    
    $self->_upload($key, sub { read($_[0], my $data, $_[2]) // confess "Error reading data $!\n"; $data }, -s $fh, $md5->hexdigest, $fh);
}

=head2 _upload

Приватный метод для upload/upload_from_file

Параметры

1) self

2) ключ

3) итератор с интерфейсом (данные, оффсет, длина). "данные" должны соответствовать последнему
параметру этой функции (т.е. (6))

4) длина данных

5) заранее высчитанный md5 от данных

6) данные. или скаляр. или filehandle

=cut


sub _upload {
    # after that $_[0] is data (scalar or filehandle)
    my ($self, $key, $iterator, $length, $md5_hex) = (shift, shift, shift, shift, shift);
    
    _check_ascii_key($key);

    if ($length > $self->{multipart_threshold}) {
        
        my $multipart = $self->{driver}->initiate_multipart_upload($key, $md5_hex);
        
        my $len = $length;
        my $offset = 0;
        my $part = 0;
        while ($offset < $len) {
            my $chunk = $iterator->($_[0], $offset, $self->{multipart_threshold});
            
            $self->{driver}->upload_part($multipart, ++$part, $chunk);
            
            $offset += $self->{multipart_threshold};
        }
        $self->{driver}->complete_multipart_upload($multipart);
    }
    else {
        $self->{driver}->upload_single_request($key, $iterator->($_[0], 0, $length));
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
    my $data;
    _download($self, $key, sub { $data .= $_[0] }) or return;
    $data;
}

=head2 download_to_file

Скачивает данные объекта с именем $key в файл $fh_or_filename.
Если объект не существует, возвращает undef (при этом выходной файл всё равно будет испорчен)
Иначе возвращает размер записанных данных.

Выходной файл открывается в режиме перезаписи, если это имя файла, если это filehandle,
это может быть append-only файл или пайпа.

Если размер объекта по факту окажется больше multisegment_threshold,
объект будет скачан несколькими запросами с заголовком Range (т.е. multi segment download).

=cut

sub download_to_file {
    my ($self, $key, $fh_or_filename) = @_;
    
    my $fh = do {
        if (ref $fh_or_filename) {
            $fh_or_filename
        }
        else {
            open my $f, ">", $fh_or_filename;
            binmode $f;
            $f;
        }
    };
    
    my $size = 0;
    _download($self, $key, sub {
        $size += length($_[0]);
        print $fh $_[0] or confess "Error writing to file $!"
    }) or return;
    $size;
}

=head2 _download

Приватный метод для download/download_to_file

Параметры:

1) self

2) имя ключа

3) appender - замыкание в которое будут передаваться данные для записи. оно должно аккумулировать их куда-то
себе или писать в файл, который оно само знает.

=cut

sub _download {
    my ($self, $key, $appender) = @_;
    
    _check_ascii_key($key);
    
    my $offset = 0;
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
        $appender->($$dataref);
        last unless $bytesleft;
    };
    if ($check_md5) {
        my $got_md5 = $md5->hexdigest;
        confess "MD5 missmatch, got $got_md5, expected $check_md5" unless $got_md5 eq $check_md5;
    }
    1;
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
