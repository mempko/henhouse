function vals = plot_key(key, secs, step, sz, kind)
  b = time;
  a = b - secs;
  ta = num2str(floor(a));
  tb = num2str(floor(b));
  u = sprintf("http://localhost:9999/values?key=%s&a=%s&b=%s&size=%d&step=%d&%s&csv", key, ta, tb, sz, step,kind);
  vs = textscan(urlread(u), "%n", "Delimiter", ",");
  vals = transpose(cell2mat(vs));
endfunction
