use v6;
use HTTP::Client;

sub MAIN($workers, $a, $z, $error-percent = 0) 
{
    my @keys = "$a".."$z";
    my @ps;

    for (1..$workers) -> $w {

        @ps.push: start { put(@keys, $error-percent); };
        @ps.push: start { get(@keys, $error-percent); };
    }

    await Promise.allof(@ps);
}

sub replace_rand_char($s is rw, @chars) 
{
    my $size = $s.chars;
    my $range = $size-1;
    my $idx = (0..$range).pick;
    my $new-char = @chars.pick;
    $s.substr-rw($idx,1) = $new-char;
}

sub corrupt_msg($msg is rw, @chars) 
{
    my $size = $msg.chars;
    my $chars = (1..$size).pick / 10;

    for 0..$chars -> $c {
        replace_rand_char($msg, @chars);
    }
}

sub put(@keys, $error-percent)
{
    my $s = IO::Socket::INET.new(:host<localhost>, :port<2003>);
    my @letters = "a".."z";
    my @nums = 1..100;
    my @stuff = <& * / : @ ~ &>;
    my @replacement-chars = @stuff.append: @letters.append: @nums;
    loop 
    {
        my $c = (0..10).pick;
        my $k = @keys.pick;
        my $msg = "{$k} {$c} {DateTime.now.posix}";
        if (0..100).pick < $error-percent {
            corrupt_msg($msg, @replacement-chars);
        }
        $s.print("$msg\n");
        sleep 0.01;

        CATCH {
            default {
                say .WHAT.perl, do given .backtrace[0] { .file, .line, .subname }
            }
        }
    }
}

sub get(@keys, $error-percent)
{
    my $http = HTTP::Client.new;
    my @letters = "a".."z";
    my @nums = 1..100;
    my @replacement-chars = @letters.append: @nums;
    loop 
    {
        my $b = DateTime.now.posix;
        my $a = $b - 120;
        my $k = @keys.pick;
        my $args = "values?a=$a&b=$b&key=$k&step=5&size=5&sum";
        if (0..100).pick < $error-percent {
            corrupt_msg($args, @replacement-chars);
        }
        my $req = "http://localhost:9999/$args";
        my $past = now;
        my $r = $http.get($req);
        my $req_time = now - $past;
        say "$req = {$r.status} {$r.content} $req_time";
        sleep 0.01;

        CATCH {
            default {
                say .WHAT.perl, do given .backtrace[0] { .file, .line, .subname }
            }
        }
    }
}
