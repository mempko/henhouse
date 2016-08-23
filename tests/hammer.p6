use v6;
use HTTP::Client;

sub MAIN($workers, $a, $z) 
{
    my @keys = "$a".."$z";
    my @ps;

    for (1..$workers) -> $w {

        @ps.push: start { put(@keys); };
        @ps.push: start { get(@keys); };
    }

    await Promise.allof(@ps);
}

sub put(@keys)
{
    my $s = IO::Socket::INET.new(:host<localhost>, :port<2003>);
    loop 
    {
        my $c = (0..10).pick;
        my $k = @keys.pick;
        $s.print("{$k} {$c} {DateTime.now.posix}\n");
        sleep 0.01;
    }
}

sub get(@keys)
{
    my $http = HTTP::Client.new;
    loop 
    {
        my $b = DateTime.now.posix;
        my $a = $b - 120;
        my $k = @keys.pick;
        my $req = "http://localhost:9999/values?a=$a&b=$b&key=$k&step=5&size=5&sum";
        my $past = now;
        my $r = $http.get($req);
        my $req_time = now - $past;
        say "$req = {$r.content} $req_time";
        sleep 0.01;
    }
}
