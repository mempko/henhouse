use v6;
use HTTP::Client;

sub MAIN(Int $workers where {($workers > 1 and $workers %% 2) or $workers == 1}, $a, $z, $error-percent = 0) 
{
    my @keys = "$a".."$z";

    if $workers < 2 
    {
        loop 
        {
            put(@keys, $error-percent, once=>True);
            get(@keys, $error-percent, once=>True);
        }
    } 
    else 
    {
        my @ps;
        my $half-workers = $workers / 2;
        for (1..$half-workers) -> $w 
        {
            @ps.push: start { put(@keys, $error-percent); };
            @ps.push: start { get(@keys, $error-percent); };
        }
        await Promise.allof(@ps);
    }

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

    for 0..$chars -> $c 
    {
        replace_rand_char($msg, @chars);
    }
}

sub put(@keys, $error-percent, :$once = False)
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
        if (0..100).pick < $error-percent 
        {
            corrupt_msg($msg, @replacement-chars);
        }
        $s.print("$msg\n");

        CATCH 
        {
            default 
            {
                say .WHAT.perl, do given .backtrace[0] { .file, .line, .subname }
            }
        }
        return if $once;
    }
}

sub get(@keys, $error-percent, :$once = False)
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
        my $size = (1..300).pick;
        my $step = (1..300).pick;
        my $args = "values?a=$a&b=$b&keys=$k&step=$step&size=$size&sum";
        if (0..100).pick < $error-percent 
        {
            corrupt_msg($args, @replacement-chars);
        }
        my $req = "http://localhost:9999/$args";
        my $past = now;
        my $r = $http.get($req);
        my $req_time = now - $past;
        say "$req = {$r.status} {$r.content} {$req_time * 1000}ms";

        CATCH 
        {
            default 
            {
                say .WHAT.perl, do given .backtrace[0] { .file, .line, .subname }
            }
        }
        return if $once;
    }
}
