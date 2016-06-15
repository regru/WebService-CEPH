use strict;
use warnings;
use Test::Spec;
use Reg::CEPH;
use File::Temp qw/tempdir/;
use Digest::MD5 qw/md5_hex/;
#
# Юнит тест с моками, тестирующий Reg::CEPH, проверяет всю логику что есть в коде.
# Все вызовы "драйвера" мокируются
#

my $tmp_dir = tempdir(CLEANUP => 1);

sub create_temp_file {
    my ($data) = @_;

    my $datafile = "$tmp_dir/data";
    open my $f, ">", $datafile or die "$!";
    print $f $data;
    close $f;
    $datafile;
}

describe CEPH => sub {
    describe constructor => sub {
        my @mandatory_params = (
                protocol => 'http',
                host => 'myhost',
                bucket => 'mybucket',
                key => 'accesskey',
                secret => 'supersecret',
        );
        my %mandatory_params_h = @mandatory_params;
        my $driver = mock();
        
        it "should work" => sub {
            Reg::CEPH::NetAmazonS3->expects('new')->with(@mandatory_params)->returns($driver);
            
            my $ceph = Reg::CEPH->new(@mandatory_params);
            
            is ref $ceph, 'Reg::CEPH';
            cmp_deeply +{%$ceph}, {
                %mandatory_params_h,
                driver_name => 'NetAmazonS3',
                multipart_threshold => 5*1024*1024,
                multisegment_threshold => 5*1024*1024,
                driver =>  $driver,
            };
        };

        for my $param (keys %mandatory_params_h) {
            it "should confess if param $param is missing" => sub {
                my %params = %mandatory_params_h;
                delete $params{$param};
                ok ! eval { Reg::CEPH->new(%params); 1 };
                like "$@", qr/Missing $param/;
            };
        }

        it "should override driver" => sub {
            Reg::CEPH::XXX->expects('new')->with(@mandatory_params)->returns($driver);
            
            my $ceph = Reg::CEPH->new(@mandatory_params, driver_name => 'XXX');
            is $ceph->{driver_name}, 'XXX';
        };

        it "should override multipart threshold" => sub {
            my $new_threshold = 10_000_000;
            Reg::CEPH::NetAmazonS3->expects('new')->with(@mandatory_params)->returns($driver);
            
            my $ceph = Reg::CEPH->new(@mandatory_params, multipart_threshold => $new_threshold);
            
            is $ceph->{multipart_threshold}, $new_threshold;
        };

        it "should override multisegment threshold" => sub {
            my $new_threshold = 10_000_000;
            Reg::CEPH::NetAmazonS3->expects('new')->with(@mandatory_params)->returns($driver);
            
            my $ceph = Reg::CEPH->new(@mandatory_params,multisegment_threshold => $new_threshold);
            
            is $ceph->{multisegment_threshold}, $new_threshold;
        };

        it "should cath bad threshold" => sub {
            ok ! eval { Reg::CEPH->new(@mandatory_params, multipart_threshold => 5*1024*1024-1); 1 };
            like "$@", qr/should be greater or eq.*MINIMAL_MULTIPART_PART/;
        };

        it "should catch extra args" => sub {
            ok ! eval { Reg::CEPH->new(@mandatory_params, abc => 42); 1; };
            like "$@", qr/Unused arguments/;
            like "$@", qr/abc.*42/;
        };
    };
    
    describe "other methods" => sub {
        my $driver = mock();
        my $ceph = bless +{ driver => $driver }, 'Reg::CEPH';
        it "should return size" => sub {
            $driver->expects('size')->with('testkey')->returns(42);
            is $ceph->size('testkey'), 42;
        };
        it "size should confess on non-ascii data" => sub {
            $driver->expects('size')->never;
            ok ! eval { $ceph->size("key\x{b5}"); 1 };
        };
        it "should delete" => sub {
            $driver->expects('delete')->with('testkey');
            $ceph->delete('testkey');
            ok 1;
        };
        it "delete should confess on non-ascii data" => sub {
            $driver->expects('delete')->never;
            ok ! eval { $ceph->delete("key\x{b5}"); 1 };
        };
    };
    describe upload => sub {
        my ($driver, $ceph, $multipart_data, $key);
        
        before each => sub {
            $driver = mock();
            $ceph = bless +{ driver => $driver, multipart_threshold => 2 }, 'Reg::CEPH';
            $multipart_data = mock();
            $key = 'mykey';
        };
        
        for my $partsdata ([qw/Aa B/], [qw/Aa Bb/], [qw/Aa Bb C/]) {
            it "multipart upload should work for @$partsdata" => sub {
                my $data_s = join('', @$partsdata);
                $driver->expects('initiate_multipart_upload')->with($key, md5_hex($data_s))->returns($multipart_data);
                my (@parts, @data);
                $driver->expects('upload_part')->exactly(scalar @$partsdata)->returns(sub{
                    my ($self, $md, $part_no, $chunk) = @_;
                    is $md+0, $multipart_data+0;
                    push @parts, $part_no;
                    push @data, $chunk;
                });
                $driver->expects('complete_multipart_upload')->with($multipart_data);
                $ceph->upload($key, $data_s);
                cmp_deeply [@parts], [map $_, 1..@$partsdata];
                cmp_deeply [@data], $partsdata;
            };
        }

        it "simple upload should work" => sub {
            $driver->expects('upload_single_request')->with($key, 'Aa');
            $ceph->upload($key, 'Aa');
            ok 1;
        };

        it "simple upload should work for less than multipart_threshold bytes" => sub {
            $driver->expects('upload_single_request')->with($key, 'A');
            $ceph->upload($key, 'A');
            ok 1;
        };

        it "simple upload should work for less than zero bytes" => sub {
            $driver->expects('upload_single_request')->with($key, '');
            $ceph->upload($key, '');
            ok 1;
        };
        it "upload should confess on non-ascii data" => sub {
            $driver->expects('upload_single_request')->never;
            ok ! eval { $ceph->upload("key\x{b5}", "a"); 1 };
        };
    };
    describe upload_from_file => sub {
        my ($driver, $ceph, $multipart_data, $key);
        
        before each => sub {
            $driver = mock();
            $ceph = bless +{ driver => $driver, multipart_threshold => 2 }, 'Reg::CEPH';
            $multipart_data = mock();
            $key = 'mykey';
        };
        
        for my $partsdata ([qw/Aa B/], [qw/Aa Bb/], [qw/Aa Bb C/]) {
            it "multipart upload should work for @$partsdata" => sub {
                my $data_s = join('', @$partsdata);
                my $datafile = create_temp_file($data_s);
                
                $driver->expects('initiate_multipart_upload')->with($key, md5_hex($data_s))->returns($multipart_data);
                my (@parts, @data);
                $driver->expects('upload_part')->exactly(scalar @$partsdata)->returns(sub{
                    my ($self, $md, $part_no, $chunk) = @_;
                    is $md+0, $multipart_data+0;
                    push @parts, $part_no;
                    push @data, $chunk;
                });
                $driver->expects('complete_multipart_upload')->with($multipart_data);
                $ceph->upload_from_file($key, $datafile);
                cmp_deeply [@parts], [map $_, 1..@$partsdata];
                cmp_deeply [@data], $partsdata;
            };
        };
        
        it "multipart upload should work for filehandle" => sub {
            my $data_s = "Hello";
            my $datafile = create_temp_file($data_s);
            open my $f, "<", $datafile or die "$!";
            
            $driver->expects('initiate_multipart_upload')->with($key, md5_hex('Hello'))->returns($multipart_data);
            my (@parts, @data);
            $driver->expects('upload_part')->exactly(3)->returns(sub{
                my ($self, $md, $part_no, $chunk) = @_;
                is $md+0, $multipart_data+0;
                push @parts, $part_no;
                push @data, $chunk;
            });
            $driver->expects('complete_multipart_upload')->with($multipart_data);
            $ceph->upload_from_file($key, $f);
            cmp_deeply [@parts], [qw/1 2 3/];
            cmp_deeply [@data], [qw/He ll o/];
        };

        it "non-multipart upload should work for filehandle" => sub {
            my $data_s = "Ab";
            my $datafile = create_temp_file($data_s);
           
            open my $f, "<", $datafile or die "$!";

            $driver->expects('upload_single_request')->with($key, 'Ab');
            $ceph->upload_from_file($key, $f);
        };

        it "non-multipart upload should work for file" => sub {
            my $data_s = "Ab";
            my $datafile = create_temp_file($data_s);
            
            $driver->expects('upload_single_request')->with($key, 'Ab');
            $ceph->upload_from_file($key, $datafile);
        };
    };
    describe download => sub {
        my ($driver, $ceph, $key);
        
        before each => sub {
            $driver = mock();
            $ceph = bless +{ driver => $driver, multisegment_threshold => 2 }, 'Reg::CEPH';
            $key = 'mykey';
        };
        
        for my $partsdata ([qw/A/], [qw/Aa/], [qw/Aa B/], [qw/Aa Bb/], [qw/Aa Bb C/]) {
            it "multisegment download should work for @$partsdata" => sub {
                my @parts = @$partsdata;
                my $md5 = md5_hex(join('', @parts));
                my $expect_offset = 0;
                $driver->expects('download_with_range')->exactly(scalar @$partsdata)->returns(sub{
                    my ($self, $key, $first, $last) = @_;
                    my $data = shift(@parts);
                    is $first, $expect_offset;
                    is $last, $first + $ceph->{multisegment_threshold};
                    $expect_offset += $ceph->{multisegment_threshold};
                    return (\$data, length(join('', @parts)), $md5, $md5);
                });
                is $ceph->download($key), join('', @$partsdata);
            };
        }
        
        it "multisegment download should crash on wrong etags" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return (\"Test", 0, "696df35ad1161afbeb6ea667e5dd5dab")
            });
            ok ! eval { $ceph->download($key); 1 };
            like "$@",
                qr/MD5 missmatch, got 0cbc6611f5540bd0809a388dc95a615b, expected 696df35ad1161afbeb6ea667e5dd5dab/;
        };

        it "multisegment download should not crash on multipart etags" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return (\"Test", 0, "696df35ad1161afbeb6ea667e5dd5dab-2861")
            });
            is $ceph->download($key), "Test";
        };

        it "multisegment download should crash on wrong custom md5" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return (\"Test", 0, "696df35ad1161afbeb6ea667e5dd5dab-2861", '42aef892dfb5a85d191e9fba6054f700')
            });
            ok ! eval { $ceph->download($key); 1 };
            like "$@",
                qr/MD5 missmatch, got 0cbc6611f5540bd0809a388dc95a615b, expected 42aef892dfb5a85d191e9fba6054f700/;
        };

        it "multisegment download should crash when etag and custom etag differs" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return (\"Test", 0, "696df35ad1161afbeb6ea667e5dd5dab", '42aef892dfb5a85d191e9fba6054f700')
            });
            ok ! eval { $ceph->download($key); 1 };
            like "$@",
                qr/ETag looks like valid md5 and x\-amz\-meta\-md5 presents but they do not match/;
        };

        it "multisegment download should return undef when object not exists" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return;
            });
            ok ! defined $ceph->download($key);
        };
        it "multisegment download should return undef if second chunk of multi segment download missed" => sub {
            $driver->expects('download_with_range')->exactly(2)->returns(sub{
                my ($self, $key, $first, $last) = @_;
                if ($first) {
                    return;
                }
                else {
                    return (\"Test", 10, "696df35ad1161afbeb6ea667e5dd5dab")
                }
            });
            ok ! defined $ceph->download($key);
        };
        it "download should confess on non-ascii data" => sub {
            $driver->expects('download_with_range')->never;
            ok ! eval { $ceph->download("key\x{b5}"); 1 };
        };
    };
    describe download_to_file => sub {
        my ($driver, $ceph, $key);
        
        before each => sub {
            $driver = mock();
            $ceph = bless +{ driver => $driver, multisegment_threshold => 2 }, 'Reg::CEPH';
            $key = 'mykey';
        };
        
        for my $partsdata ([qw/A/], [qw/Aa/], [qw/Aa B/], [qw/Aa Bb/], [qw/Aa Bb C/]) {
            it "multisegment download should work for @$partsdata" => sub {
                my $datafile = "$tmp_dir/datafile";
                
                my @parts = @$partsdata;
                my $md5 = md5_hex(join('', @parts));
                my $expect_offset = 0;
                $driver->expects('download_with_range')->exactly(scalar @$partsdata)->returns(sub{
                    my ($self, $key, $first, $last) = @_;
                    my $data = shift(@parts);
                    is $first, $expect_offset;
                    is $last, $first + $ceph->{multisegment_threshold};
                    $expect_offset += $ceph->{multisegment_threshold};
                    return (\$data, length(join('', @parts)), $md5, $md5);
                });
                my $data = join('', @$partsdata);
                is $ceph->download_to_file($key, $datafile), length $data;
                open my $f, "<", $datafile;
                binmode $f;
                my @data_a = <$f>;
                my $data_s = join('', @data_a);
                is $data_s, $data;
            };
        }
        it "multisegment download should work for filehanlde" => sub {
            my $datafile = "$tmp_dir/datafile";
            my $data = "Ab";
            my $md5 = md5_hex('Ab');
            my $expect_offset = 0;
            $driver->expects('download_with_range')->returns(sub{
                my ($self, $key, $first, $last) = @_;
                return (\"Ab", 0, $md5, $md5);
            });
            is $ceph->download_to_file($key, $datafile), 2;
            open my $f, "<", $datafile;
            binmode $f;
            my @data_a = <$f>;
            my $data_s = join('', @data_a);
            is $data_s, 'Ab';
        };
        it "download to file should return undef when object not exists" => sub {
            $driver->expects('download_with_range')->exactly(1)->returns(sub{
                return;
            });
            ok ! defined $ceph->download_to_file($key, "$tmp_dir/datafile");
        };
    };
};

runtests unless caller;
