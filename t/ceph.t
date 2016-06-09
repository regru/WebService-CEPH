use strict;
use warnings;
use Test::Spec;
use Reg::CEPH;

#
# Юнит тест с моками, тестирующий Reg::CEPH, проверяет всю логику что есть в коде.
# Все вызовы "драйвера" мокируются
#

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

        it "should override threshold" => sub {
            my $new_threshold = 10_000_000;
            Reg::CEPH::NetAmazonS3->expects('new')->with(@mandatory_params)->returns($driver);
            
            my $ceph = Reg::CEPH->new(@mandatory_params, multipart_threshold => $new_threshold);
            
            is $ceph->{multipart_threshold}, $new_threshold;
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
    
    describe methods => sub {
        my $driver = mock();
        my $ceph = bless +{ driver => $driver }, 'Reg::CEPH';
        it "should download" => sub {
            $driver->expects('download')->with('testkey')->returns("testdata");
            is $ceph->download('testkey'), "testdata";
        };
        it "should return size" => sub {
            $driver->expects('size')->with('testkey')->returns(42);
            is $ceph->size('testkey'), 42;
        };
        it "should delete" => sub {
            $driver->expects('delete')->with('testkey');
            $ceph->delete('testkey');
            ok 1;
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
                $driver->expects('initiate_multipart_upload')->with($key)->returns($multipart_data);
                my (@parts, @data);
                $driver->expects('upload_part')->exactly(scalar @$partsdata)->returns(sub{
                    my ($self, $md, $part_no, $chunk) = @_;
                    is $md+0, $multipart_data+0;
                    push @parts, $part_no;
                    push @data, $chunk;
                });
                $driver->expects('complete_multipart_upload')->with($multipart_data);
                $ceph->upload($key, join('', @$partsdata));
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
    };
};

runtests unless caller;
