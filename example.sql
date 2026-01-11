begin
  p_queue.register_q('QS', 5);
end;

begin
  for i in 1..100000 loop
    p_queue.enq('QS', hextoraw('A'), p_use_pipe => 'Y');
  end loop;
end;

declare
  l_id raw(16);
  l_payload blob;
begin
  for i in 1..100000 loop
    p_queue.deq('QS', 1, l_payload, p_use_pipe => 'Y');
  end loop;
end;

declare
  l_id raw(16);
  l_payload_array p_queue.te_payload_t := p_queue.te_payload_t();
begin
  for i in 1..1 loop
    l_payload_array.extend;
    l_payload_array(i) := hextoraw('A');
  end loop;
  for i in 1..100000 loop
    p_queue.enq_array('QS', l_payload_array);
    commit;
  end loop;
end;

declare
  l_id raw(16);
  l_payload_array p_queue.te_payload_t;
begin
  for i in 1..100000 loop
    p_queue.deq_array('QS', 1, l_payload_array, p_size => 1);
    commit;
  end loop;
end;

declare
  l_id raw(16);
  l_payload blob;
begin
  for i in 1..100000 loop
    p_queue.deq('QS', 1, l_payload, p_prefetch => 1);
    commit;
  end loop;
end;

enq_array (100000):
100 - 8.46ñ
50  - 8.58ñ
25  - 8.69ñ
10  - 9.15ñ
5   - 10.00ñ
2   - 12.70ñ
1   - 16.38ñ

enq (100000)
persistent - 16.69ñ (16.56ñ dbms_aq)
use_pipe   -  0.74ñ (1.52ñ  dbms_aq buffered)

deq_array(100000)
100 - 5.67c
50  - 5.79c
25  - 6.86c
10  - 7.98c
5   - 10.88c
2   - 18.02c
1   - 21.65c

deq(100000)
prefetch
100 - 16.30c (11.49c dbms_aq)
50  - 16.35c
25  - 17.40c
10  - 16.83c
5   - 17.67c
2   - 20.58c
1   - 21.91c 
use_pipe - 1.08c (1.74c  dbms_aq buffered)


