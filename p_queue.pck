create or replace package p_queue 
is
  
  -- Исключение при переполнении всех каналов данных (можно сделать паузу).
  high_enq_rate exception;
  pragma exception_init(high_enq_rate, -25307);
  
  -- Таблица сообщений.
  type te_payload_t is table of blob;
  
  /**
   * Регистрация новой очереди.
   * @param p_qname Наименование очереди.
   * @param p_try_count Максимальное число попыток извлечений.
   * @param p_try_delay Задержка повторной попытки извлечения.
   * @param p_low_latency Минимальная задержка извлечения.
   * @param p_enqueue Отправка включена.
   * @param p_dequeue Извлечение включено.
   */
  procedure register_q(p_qname in varchar2,
                       p_try_count in pls_integer default 5,
                       p_try_delay in number default 0,
                       p_low_latency in varchar2 default 'N',
                       p_enqueue in varchar2 default 'Y',
                       p_dequeue in varchar2 default 'Y');
  
  /**
   * Обновление параметров очереди.
   * @param p_qname Наименование очереди.
   * @param p_try_count Максимальное число попыток извлечений.
   * @param p_try_delay Задержка повторной попытки извлечения.
   * @param p_low_latency Минимальная задержка извлечения.
   * @param p_enqueue Отправка включена.
   * @param p_dequeue Извлечение включено.
   */
  procedure update_q(p_qname in varchar2,
                     p_try_count in pls_integer default null,
                     p_try_delay in number default null,
                     p_low_latency in varchar2 default null,
                     p_enqueue in varchar2 default null,
                     p_dequeue in varchar2 default null);
  
  /**
   * Удаление очереди.
   * @param p_qname Наименование очереди.
   */
  procedure drop_q(p_qname in varchar2);
  
  /**
   * Отправка массива сообщений в очередь (канал данных не поддерживается).
   * @param p_qname Наименование очереди.
   * @param p_payload_array Массив сообщения.
   * @param p_immediate Немедленная отправка в очередь (автономная транзакция).
   * @param p_priority Приоритет сообщения.
   * @param p_delay Задержка видимости сообщения.
   * @param p_expire Время жизни сообщения.
   */
  procedure enq_array(p_qname in varchar2,
                      p_payload_array in te_payload_t,
                      p_immediate in varchar2 default 'N',
                      p_priority in integer default 1,
                      p_delay in number default 0,
                      p_expire in number default 0);
  
  /**
   * Отправка сообщения в очередь.
   * @param p_qname Наименование очереди.
   * @param p_payload Сообщение.
   * @param p_use_pipe Использование канала данных.
   * @param p_immediate Немедленная отправка в очередь (автономная транзакция).
   * @param p_priority Приоритет сообщения.
   * @param p_delay Задержка видимости сообщения.
   * @param p_expire Время жизни сообщения.
   */
  procedure enq(p_qname in varchar2,
                p_payload in blob,
                p_use_pipe in varchar2 default 'N',
                p_immediate in varchar2 default 'N',
                p_priority in integer default 1,
                p_delay in number default 0,
                p_expire in number default 0);
  
  /**
   * Получить идентификаторы отправленных сообщений.
   * @param p_id Порядковый номер сообщения (для массива).
   * @return Идентификатор отправленного сообщения.
   */
  function enq_id(p_id in pls_integer default 1) return raw;
  
  /**
   * Извлечение массива сообщений из очереди (канал данных не поддерживается).
   * @param p_qname Наименование очереди.
   * @param p_wait Время ожидания сообщений.
   * @param p_payload_array Массив сообщений.
   * @param p_immediate Немедленное извлечение из очереди (автономная транзакция).
   * @param p_size Число сообщений для извлечения.
   * @param p_prefetch Число предзагружаемых сообщений в сессии (оптимизация чтения).
   */
  procedure deq_array(p_qname in varchar2, 
                      p_wait in number,
                      p_payload_array out nocopy te_payload_t,
                      p_immediate in varchar2 default 'N',
                      p_size in pls_integer default 1,
                      p_prefetch in pls_integer default 1);
  
  /**
   * Извлечение сообщения из очереди.
   * @param p_qname Наименование очереди.
   * @param p_wait Время ожидания сообщения.
   * @param p_payload Сообщение.
   * @param p_payload_id Заданный идентификатор сообщения.
   * @param p_use_pipe Использование канала данных.
   * @param p_immediate Немедленное извлечение из очереди (автономная транзакция).
   * @param p_prefetch Число предзагружаемых сообщений в сессии (оптимизация чтения).
   */
  procedure deq(p_qname in varchar2, 
                p_wait in number,
                p_payload out nocopy blob,
                p_payload_id in raw default null,
                p_use_pipe in varchar2 default 'N',
                p_immediate in varchar2 default 'N',
                p_prefetch in pls_integer default 1);
  
  /**
   * Получить идентификаторы извлеченных сообщений.
   * @param p_id Порядковый номер сообщения (для массива).
   * @return Идентификатор извлеченного сообщения.
   */
  function deq_id(p_id in pls_integer default 1) return raw;
  
  /**
   * Процесс отслеживания времени жизни сообщений и их возврата в обработку.
   * @param p_qname Наименование очереди.
   */           
  procedure monitor(p_qname in varchar2 default null);
  
  /**
   * Процесс очистки индекса очереди от удаленных сообщений.
   * В случае отсутствия обслуживания может произойти существенное замедление извлечения новых сообщений.
   */
  procedure maintenance;

end;
/
create or replace package body p_queue
is

  c_schema constant varchar2(128) := sys_context('userenv', 'current_schema');
  
  -- Статус сообщения.
  c_state_ready constant integer := 0; -- Доступно для извлечения.
  c_state_dequeued constant integer := 1; -- Извлечено для обработки.
  c_state_expired constant integer := 2; -- Время жизни истекло.

  c_data_table constant varchar2(128) := 'T_QUEUE_DATA';
  c_data_index constant varchar2(128) := 'QEDA_SEPYEE_IDX';
  
  c_notify_pipe_size constant integer := 1024; -- Размер канала данных уведомлений.
  c_pipe_size constant pls_integer := 2097152; -- Размер каждого канала данных.
  c_pipe_count constant pls_integer := 12; -- Число каналов данных.
  c_prefetch_period constant number := 5; -- Время резервирования предзагруженных сообщений.
  c_act_period constant number := 1;
  
  -- Очереди и их параметры.
  type te_queue is record(id integer,
                          hex varchar2(15),
                          qname varchar2(128),
                          queue_pipe varchar2(128),
                          notify_pipe varchar2(128),
                          try_count integer,
                          try_delay number,
                          low_latency varchar2(1),
                          enqueue varchar2(1),
                          dequeue varchar2(1),
                          expire date);
  type te_queue_t is table of te_queue index by varchar2(128);
  g_queue_t te_queue_t;
  g_queue te_queue;
  -- Период обновления параметров очереди.
  c_queue_delay constant number := 5/86400;
  
  -- Сообщения прочитанные с избытком.
  type te_prefetch_t is table of blob index by varchar2(47);
  g_prefetch_t te_prefetch_t;
  
  -- Активные транзакции базы данных.
  type te_xid_t is table of integer index by varchar2(30);
  g_xid_t te_xid_t;
  
  -- Идентификатор сообщения.
  type te_id_t is table of raw(16) index by pls_integer;
  g_enq_id_t te_id_t;
  g_deq_id_t te_id_t;
  
  procedure throw(p_n in integer, p_message in varchar2)
  is
  begin
    raise_application_error(-20200 - p_n, p_message, true);
  end;
  
  function seconds(p_a in timestamp, p_b in timestamp) return number
  is
  begin
    return extract(day from (p_a - p_b)) * 86400 +
           extract(hour from (p_a - p_b)) * 3600 +
           extract(minute from (p_a - p_b)) * 60 +
           extract(second from (p_a - p_b));
  end;
  
  procedure load_xid_t
  is
  begin
    g_xid_t.delete;
    for i in (select to_char(xidusn) || to_char(xidslot) || to_char(xidsqn) xid 
                from v$transaction) loop
      g_xid_t(i.xid) := null;
    end loop;
  end;
  
  function get_xid(p_create in boolean default false) return varchar2
  is
  begin
    return dbms_transaction.local_transaction_id(p_create);
  end;
  
  function check_xid(p_xid in varchar2) return varchar2
  is
  begin
    if g_xid_t.exists(p_xid) then
      return 'Y';
    else
      return 'N';
    end if;
  end;
  
  procedure load_queue(p_qname in varchar2, p_time in date)
  is
  begin
    if p_qname is null then
      throw(1, 'Имя очереди не задано');
    end if;
    g_queue := null;
    begin
      select q.id, q.try_count, q.try_delay, q.low_latency, q.enqueue, q.dequeue
        into g_queue.id, 
             g_queue.try_count, 
             g_queue.try_delay,
             g_queue.low_latency,
             g_queue.enqueue, 
             g_queue.dequeue
        from t_queue q
       where q.name = p_qname;
    exception
      when no_data_found then
        throw(2, 'Очередь (' || p_qname || ') не найдена');
    end;
    g_queue.hex := to_char(g_queue.id, 'fm0xxxxxxxxxxxxxx');
    g_queue.qname := p_qname;
    g_queue.queue_pipe := 'QB_' || upper(g_queue.hex) || '_';
    g_queue.notify_pipe := 'QN_' || upper(g_queue.hex);
    g_queue.expire := p_time + c_queue_delay; -- Интервал обновления параметров.
    g_queue_t(p_qname) := g_queue;
  end;
  
  procedure init_queue(p_qname in varchar2)
  is
    c_time constant date := sysdate;
  begin
    if g_queue.qname is null or not g_queue.qname = p_qname then
      if g_queue_t.exists(p_qname) then
        g_queue := g_queue_t(p_qname);
      else
        load_queue(p_qname, c_time);
      end if;
    end if;
    if g_queue.qname = p_qname then
      if g_queue.expire <= c_time then
        load_queue(p_qname, c_time);
      end if;
    end if;
  end;
  
  procedure register_q(p_qname in varchar2,
                       p_try_count in pls_integer default 5,
                       p_try_delay in number default 0,
                       p_low_latency in varchar2 default 'N',
                       p_enqueue in varchar2 default 'Y',
                       p_dequeue in varchar2 default 'Y')
  is
  begin
    begin
      insert into t_queue(id, name, try_count, try_delay, low_latency, enqueue, dequeue) 
           values (seq_qid.nextval, p_qname, coalesce(p_try_count, 5), coalesce(p_try_delay, 0), 
                                    coalesce(p_low_latency, 'N'), coalesce(p_enqueue, 'Y'), coalesce(p_dequeue, 'Y'));
    exception
      when dup_val_on_index then
        throw(3, 'Очередь (' || p_qname || ') уже существует');
    end;
  end;
  
  procedure update_q(p_qname in varchar2,
                     p_try_count in pls_integer default null,
                     p_try_delay in number default null,
                     p_low_latency in varchar2 default null,
                     p_enqueue in varchar2 default null,
                     p_dequeue in varchar2 default null)
  is
  begin
    init_queue(p_qname);
    update t_queue q
       set q.try_count = coalesce(p_try_count, q.try_count),
           q.try_delay = coalesce(p_try_delay, q.try_delay),
           q.low_latency = coalesce(p_low_latency, q.low_latency),
           q.enqueue = coalesce(p_enqueue, q.enqueue),
           q.dequeue = coalesce(p_dequeue, q.dequeue)
     where q.id = g_queue.id;
  end;
  
  procedure drop_q(p_qname in varchar2)
  is
  begin
    init_queue(p_qname);
    delete from t_queue where id = g_queue.id;
  end;

  /**
   * Уведомление о новом сообщении в очереди.
   * @param p_qname Наименование очереди.
   * @param p_done После завершения транзакции.
   */
  procedure ready_notify(p_done in varchar2, p_size in pls_integer default 1)
  is
    l_status integer;
  begin
    for i in 1 .. p_size loop
      dbms_pipe.reset_buffer;
      dbms_pipe.pack_message(p_done);
      l_status := dbms_pipe.send_message(g_queue.notify_pipe, 0, c_notify_pipe_size);
    end loop;
  end;
  
  procedure enq_pipe(p_payload in blob)
  is
    l_enq_time timestamp(6);
    l_s pls_integer;
    l_n pls_integer;
    l_pipe varchar2(128);
    l_status integer;
  begin
    dbms_pipe.reset_buffer;
    dbms_pipe.pack_message_raw(p_payload);
    l_enq_time := systimestamp() at time zone 'utc';
    l_s := dbms_utility.get_hash_value(l_enq_time, 1, c_pipe_count);
    l_n := l_s;
    loop
      l_pipe := g_queue.queue_pipe || to_char(l_n, 'fm0x');
      l_status := dbms_pipe.send_message(l_pipe, timeout => 0, maxpipesize => c_pipe_size);
      exit when l_status = 0;
      if l_status = 1 then
        l_n := l_n + 1;
        if l_n >= c_pipe_count then
          l_n := 1;
        end if;
        if l_n = l_s then
          raise high_enq_rate;
        end if;
      else
        throw(4, 'При отправке сообщения в канал данных (' || l_pipe || ') получен статус (' || l_status || ')');
      end if;
    end loop;
  end;

  procedure enq(p_priority in integer,
                p_enq_time in timestamp,
                p_payload in blob,
                p_payload_array in te_payload_t,
                p_expire_time in timestamp,
                p_notify in varchar2 default 'N')
  is
  begin
    if p_payload_array is null then
      insert into t_queue_data(qid, state, priority, enq_time, id, payload, expire_time)
             values (g_queue.id, c_state_ready, p_priority, p_enq_time, sys_guid(), p_payload, p_expire_time)
          returning id bulk collect into g_enq_id_t;
    else
      forall i in indices of p_payload_array
        insert into t_queue_data(qid, state, priority, enq_time, id, payload, expire_time)
             values (g_queue.id, c_state_ready, p_priority, p_enq_time, sys_guid(), p_payload_array(i), p_expire_time)
          returning id bulk collect into g_enq_id_t;
    end if;
    if p_notify = 'Y' then
      ready_notify('N', g_enq_id_t.count);
    end if;
  end;
  
  procedure enq_at(p_priority in integer,
                   p_enq_time in timestamp,
                   p_payload in blob,
                   p_payload_array in te_payload_t,
                   p_expire_time in timestamp)
  is
    pragma autonomous_transaction;
  begin
    enq(p_priority, p_enq_time, p_payload, p_payload_array, p_expire_time);
    commit;
    if p_payload_array is null then
      ready_notify('Y');
    else
      ready_notify('Y', p_payload_array.count);
    end if;
  end;
  
  procedure enq_int(p_qname in varchar2,
                    p_payload in blob,
                    p_payload_array in te_payload_t,
                    p_use_pipe in varchar2 default 'N',
                    p_immediate in varchar2 default 'N',
                    p_priority in integer default 1,
                    p_delay in number default 0,
                    p_expire in number default 0)
  is
    l_enq_time timestamp(6);
    l_expire_time timestamp(6);
  begin
    init_queue(p_qname);
    if not g_queue.enqueue = 'Y' then
      throw(5, 'Отправка сообщений отключена (' || p_qname || ')');
    end if;
    if p_expire < p_delay then
      throw(6, 'Время жизни сообщений не может наступать ранее видимости');
    end if;
    if p_use_pipe = 'Y' then
      enq_pipe(p_payload);
    else
      l_enq_time := systimestamp() at time zone 'utc';
      if p_expire > 0 then
        l_expire_time := l_enq_time + numtodsinterval(p_expire, 'second');
      end if;
      if p_delay > 0 then
        l_enq_time := l_enq_time + numtodsinterval(p_delay, 'second');
      end if;
      if p_immediate = 'Y' then
        enq_at(p_priority, l_enq_time, p_payload, p_payload_array, l_expire_time);
      else
        enq(p_priority, l_enq_time, p_payload, p_payload_array, l_expire_time, 'Y');
      end if;
    end if;
  end;
  
  procedure enq_array(p_qname in varchar2,
                      p_payload_array in te_payload_t,
                      p_immediate in varchar2 default 'N',
                      p_priority in integer default 1,
                      p_delay in number default 0,
                      p_expire in number default 0)
  is
  begin
    if p_payload_array is null then
      throw(7, 'Массив сообщений не задан');
    end if;
    enq_int(p_qname,
            null,
            p_payload_array, 
            p_use_pipe => 'N',
            p_immediate => p_immediate,
            p_priority => p_priority,
            p_delay => p_delay,
            p_expire => p_expire);
  end;
  
  procedure enq(p_qname in varchar2,
                p_payload in blob,
                p_use_pipe in varchar2 default 'N',
                p_immediate in varchar2 default 'N',
                p_priority in integer default 1,
                p_delay in number default 0,
                p_expire in number default 0)
  is
  begin
    enq_int(p_qname, 
            p_payload,
            null,
            p_use_pipe => p_use_pipe,
            p_immediate => p_immediate,
            p_priority => p_priority,
            p_delay => p_delay,
            p_expire => p_expire);
  end;
  
  function enq_id(p_id in pls_integer default 1) return raw
  is
    c_id constant pls_integer := coalesce(p_id, 1);
  begin
    if g_enq_id_t.exists(c_id) then
      return g_enq_id_t(c_id);
    else
      return null;
    end if;
  end;
  
  function wait_notify(p_wait in number) return varchar2
  is
    l_status integer;
    l_notify varchar2(1);
  begin
    l_status := dbms_pipe.receive_message(g_queue.notify_pipe, p_wait);
    if l_status = 0 then
      dbms_pipe.unpack_message(l_notify);
      return l_notify;
    end if;
    return null;
  end;
  
  function deq_pipe(p_pipe in varchar2, p_wait in integer, p_payload out nocopy blob) return boolean
  is
    l_status integer;
  begin
    l_status := dbms_pipe.receive_message(p_pipe, timeout => p_wait);
    if l_status = 0 then
      dbms_pipe.unpack_message_raw(p_payload);
      return true;
    elsif l_status > 1 then
      throw(8, 'При ожиданнии ответа из канала данных (' || p_pipe || ') получен статус (' || l_status || ')');
    end if;
    return false;
  end;
  
  procedure deq_pipe(p_wait in number, p_payload out nocopy blob)
  is
    l_deq_time timestamp(6);
    l_wait_time timestamp(6);
    l_s pls_integer;
    l_n pls_integer;
    l_wait integer := 0;
  begin
    l_deq_time := systimestamp() at time zone 'utc';
    l_wait_time := l_deq_time + numtodsinterval(p_wait, 'second');
    l_s := dbms_utility.get_hash_value(l_deq_time, 1, c_pipe_count);
    l_n := l_s;
    loop
      exit when deq_pipe(g_queue.queue_pipe || to_char(l_n, 'fm0x'), l_wait, p_payload);
      l_wait := 0;
      l_n := l_n + 1;
      if l_n >= c_pipe_count then
        l_n := 1;
      end if;
      if l_n = l_s then
        l_wait := least(p_wait, 1);
        l_deq_time := systimestamp() at time zone 'utc';
        exit when l_deq_time >= l_wait_time;
      end if;
    end loop;
  end;
  
  procedure deq(p_deq_time in timestamp,
                p_xid in varchar2,
                p_payload_id in raw,
                p_immediate in varchar2,
                p_size in pls_integer,
                p_prefetch in pls_integer,
                p_payload out nocopy blob,
                p_payload_array out nocopy te_payload_t)
  is
    l_limit pls_integer;
    type te_data is record(qid integer, 
                           state integer, 
                           id raw(16), 
                           payload blob, 
                           expire_time timestamp(6), 
                           deq_xid varchar2(30));
    type te_data_t is table of te_data;
    l_idx varchar2(47);
    l_id_t te_id_t;
    l_n pls_integer := 0;
    l_prefetch_time timestamp(6);
    l_deq_xid varchar2(30) := p_xid;
    l_deq_id_t te_id_t;
    l_c sys_refcursor;
    l_data_t te_data_t;
    l_del_t te_id_t;
  begin 
    -- Обработка предзагруженных сообщений.
    if g_prefetch_t.count > 0 then
      if p_payload_id is null then
        l_idx := g_prefetch_t.next(g_queue.hex);
      else
        l_idx := g_queue.hex || hextoraw(p_payload_id);
        if not g_prefetch_t.exists(l_idx) then
          l_idx := null;
        end if;
      end if;
      loop
        exit when l_idx is null or not substr(l_idx, 1, 15) = g_queue.hex;
        l_id_t(l_id_t.count + 1) := coalesce(p_payload_id, hextoraw(substr(l_idx, 16)));
        if p_payload_id is null then
          l_idx := g_prefetch_t.next(l_idx);
        else
          l_idx := null;
        end if;        
        if l_idx is null or l_n + l_id_t.count >= p_size then
          if l_prefetch_time is null then
            l_prefetch_time := p_deq_time - numtodsinterval(c_prefetch_period, 'second');
          end if;
          if l_deq_xid is null and not l_idx is null then
            l_deq_xid := get_xid(p_create => true);
          end if;
          if l_id_t.count = 1 then
            update /*+ index(qd qeda_id_idx) */
                   t_queue_data qd
               set qd.deq_xid = l_deq_xid
             where qd.qid = g_queue.id
               and qd.state = c_state_dequeued
               and (qd.expire_time is null or qd.expire_time >= p_deq_time)
               and qd.deq_time >= l_prefetch_time
               and qd.id = l_id_t(1)
            returning qd.id bulk collect into l_deq_id_t;
          else
            -- Проверяем сообщения.
            forall i in indices of l_id_t
              update /*+ index(qd qeda_id_idx) */
                     t_queue_data qd
                 set qd.deq_xid = l_deq_xid
               where qd.qid = g_queue.id
                 and qd.state = c_state_dequeued
                 and (qd.expire_time is null or qd.expire_time >= p_deq_time)
                 and qd.deq_time >= l_prefetch_time
                 and qd.id = l_id_t(i)
              returning qd.id bulk collect into l_deq_id_t;
          end if;
          if l_deq_id_t.count > 0 then
            for i in 1 .. l_deq_id_t.count loop
              l_n := l_n + 1;
              if p_size = 1 then
                p_payload := g_prefetch_t(g_queue.hex || rawtohex(l_deq_id_t(i)));
              else
                if p_payload_array is null then
                  p_payload_array := te_payload_t();
                end if;
                p_payload_array.extend;
                p_payload_array(l_n) := g_prefetch_t(g_queue.hex || rawtohex(l_deq_id_t(i)));
              end if;
              g_deq_id_t(l_n) := l_deq_id_t(i);
            end loop;
          end if;
          for i in 1 .. l_id_t.count loop
            g_prefetch_t.delete(g_queue.hex || rawtohex(l_id_t(i)));
          end loop;
          if l_n >= p_size then
            return;
          end if;
          l_id_t.delete;
        end if;
      end loop;
    end if;
    if p_payload_id is null then
      open l_c for
        select /*+ index_asc(qd qeda_sepyee_idx) */
               qd.qid, qd.state, qd.id, qd.payload, qd.expire_time, qd.deq_xid
          from t_queue_data qd
         where qd.qid = g_queue.id
           and qd.state = c_state_ready
           and qd.enq_time <= p_deq_time
         order by qd.state, qd.priority, qd.enq_time for update skip locked;
      l_limit := p_size - l_n + p_prefetch;
    else
      -- Извлечение сообщения с заданным идентификатором.
      open l_c for
        select /*+ index(qd qeda_id_idx) */
               qd.qid, qd.state, qd.id, qd.payload, qd.expire_time, qd.deq_xid
          from t_queue_data qd
         where qd.qid = g_queue.id
           and qd.id = p_payload_id
           and qd.state = c_state_ready
           and qd.enq_time <= p_deq_time for update skip locked;
      l_limit := 1;
    end if;
    fetch l_c bulk collect into l_data_t limit l_limit;
    if l_data_t.count > 0 then
      if l_deq_xid is null then
        l_deq_xid := get_xid; -- Уже есть активная транзакция.
      end if;
      for i in 1 .. l_data_t.count loop
        if l_data_t(i).expire_time < p_deq_time then -- Истекло время жизни.
          l_data_t(i).qid := -g_queue.id;
          l_data_t(i).state := c_state_expired;
        elsif l_n < p_size or i = 1 then -- Сообщения для извлечения.
          l_n := l_n + 1;
          if p_size = 1 then
            p_payload := l_data_t(i).payload;
          else
            if p_payload_array is null then
              p_payload_array := te_payload_t();
            end if;
            p_payload_array.extend;
            p_payload_array(l_n) := l_data_t(i).payload;
          end if;
          g_deq_id_t(l_n) := l_data_t(i).id;
          if p_immediate = 'Y' then
            l_del_t(l_del_t.count + 1) := l_data_t(i).id;
            l_data_t.delete(i);
          else
            l_data_t(i).state := c_state_dequeued;
            l_data_t(i).deq_xid := l_deq_xid;
          end if;
        else -- Предзагруженные сообщения.
          l_data_t(i).state := c_state_dequeued;
          g_prefetch_t(g_queue.hex || rawtohex(l_data_t(i).id)) := l_data_t(i).payload;
        end if;
      end loop;
      if l_del_t.count = 1 then
        delete  /*+ index(qd qeda_id_idx) */
          from t_queue_data qd
         where qd.qid = g_queue.id 
           and qd.id = l_del_t(1);
      elsif l_del_t.count > 1 then
        forall i in indices of l_del_t
          delete  /*+ index(qd qeda_id_idx) */
            from t_queue_data qd
           where qd.qid = g_queue.id 
             and qd.id = l_del_t(i);
      end if;
      if l_data_t.count = 1 then
        update /*+ index(qd qeda_id_idx) */
               t_queue_data qd
           set qd.qid = l_data_t(1).qid,
               qd.state = l_data_t(1).state,
               qd.deq_xid = l_data_t(1).deq_xid,
               qd.deq_time = p_deq_time
         where qd.qid = g_queue.id 
           and qd.id = l_data_t(1).id;
      elsif l_data_t.count > 1 then
        forall i in indices of l_data_t
          update /*+ index(qd qeda_id_idx) */
                 t_queue_data qd
             set qd.qid = l_data_t(i).qid,
                 qd.state = l_data_t(i).state,
                 qd.deq_xid = l_data_t(i).deq_xid,
                 qd.deq_time = p_deq_time
           where qd.qid = g_queue.id 
             and qd.id = l_data_t(i).id;
      end if;
    end if;
    if p_xid is null then
      commit;
    end if;
    close l_c;
  exception
    when others then
      if l_c%isopen then
        close l_c;
      end if;
      raise;
  end;
  
  procedure deq_at(p_deq_time in timestamp, 
                   p_xid in varchar2,
                   p_payload_id in raw,
                   p_immediate in varchar2,
                   p_size in pls_integer,
                   p_prefetch in pls_integer,
                   p_payload out nocopy blob,
                   p_payload_array out nocopy te_payload_t)
  is
    pragma autonomous_transaction;
  begin
    deq(p_deq_time, p_xid, p_payload_id, p_immediate, p_size, p_prefetch, p_payload, p_payload_array);
    commit;
  end;
  
  procedure deq_int(p_qname in varchar2, 
                    p_wait in number,
                    p_payload out nocopy blob,
                    p_payload_array out nocopy te_payload_t,
                    p_payload_id in raw,
                    p_use_pipe in varchar2,
                    p_immediate in varchar2,
                    p_size in pls_integer,
                    p_prefetch in pls_integer)
  is
    l_xid varchar2(30);
    l_wait_time timestamp(6);
    l_deq_time timestamp(6);
    l_notify varchar2(1);
    l_wait number := 0;
    l_act_time timestamp(6);
  begin
    init_queue(p_qname);
    if not g_queue.dequeue = 'Y' then
      throw(9, 'Извлечение сообщений отключено (' || p_qname || ')');
    end if;
    if p_size < 1 then
      throw(10, 'Число сообщений для извлечения должно быть больше нуля (' || p_size || ')');
    end if;
    if p_prefetch < 1 then
      throw(11, 'Число предзагружаемых сообщений должно быть больше нуля (' || p_prefetch || ')');
    end if;
    if p_use_pipe = 'Y' then -- Использование канала данных.
      if not p_payload_id is null then
        throw(12, 'Извлечение сообщения по идентификатору из канала данных не поддерживается');
      end if;
      deq_pipe(p_wait, p_payload);
      return;
    end if;
    l_xid := get_xid; -- Текущая транзакция.
    l_deq_time := systimestamp() at time zone 'utc';
    l_wait_time := l_deq_time + numtodsinterval(p_wait, 'second'); -- Время ожидания.
    g_deq_id_t.delete; -- Идентификаторы извлеченных сообщений.
    loop -- Цикл извлечения сообщений.
      if p_immediate = 'Y' or not l_xid is null then -- Немедленное извлечение или в активной транзакции.
        deq_at(l_deq_time, l_xid, p_payload_id, p_immediate, p_size, p_prefetch, p_payload, p_payload_array);
      else
        deq(l_deq_time, l_xid, p_payload_id, p_immediate, p_size, p_prefetch, p_payload, p_payload_array);
      end if;
      if g_deq_id_t.count > 0 then -- Извлечены сообщения.
        if p_immediate is null or not p_immediate = 'Y' then
          if p_size = 1 then
            delete /*+ cache_cb(qd) index(qd qeda_id_idx) */
              from t_queue_data qd
             where qd.qid = g_queue.id
               and qd.id = g_deq_id_t(1);
          else
            -- Заранее удаляем сообщения.
            forall i in indices of g_deq_id_t
              delete /*+ cache_cb(qd) index(qd qeda_id_idx) */
                from t_queue_data qd
               where qd.qid = g_queue.id
                 and qd.id = g_deq_id_t(i);
          end if;
        end if;
        exit;
      end if;
      l_deq_time := systimestamp() at time zone 'utc';
      if l_deq_time >= l_wait_time then
        if not p_payload_id is null then
          throw(13, 'Сообщение с идентификатором (' || p_payload_id || ') не найдено');
        end if;
        exit;
      end if;
      -- Управление ожиданиеми.
      if g_queue.low_latency = 'Y' and l_notify = 'N' then -- Активное ожидание.
        if l_deq_time < l_act_time then
          dbms_lock.sleep(0.01); -- Минимальная пауза.
        else
          l_notify := null;
        end if;
      else
        l_notify := wait_notify(l_wait);
        if l_notify is null then
          l_wait := 1;
        else
          l_wait := 0;
          if g_queue.low_latency = 'Y' and l_notify = 'N' then -- Активное ожидание.
            l_act_time := l_deq_time + numtodsinterval(c_act_period, 'second');
          end if;
        end if;
      end if;
    end loop;
  end;
  
  procedure deq_array(p_qname in varchar2, 
                      p_wait in number,
                      p_payload_array out nocopy te_payload_t,
                      p_immediate in varchar2 default 'N',
                      p_size in pls_integer default 1,
                      p_prefetch in pls_integer default 1)
  is
    c_size constant pls_integer := coalesce(p_size, 1);
    c_prefetch constant pls_integer := coalesce(p_prefetch, 1);
    l_payload blob;
  begin
    deq_int(p_qname, p_wait, l_payload, p_payload_array, null, 'N', p_immediate, c_size, c_prefetch);
    if c_size = 1 then
      p_payload_array := te_payload_t(l_payload);
    end if;
  end;
  
  procedure deq(p_qname in varchar2, 
                p_wait in number,
                p_payload out nocopy blob,
                p_payload_id in raw default null,
                p_use_pipe in varchar2 default 'N',
                p_immediate in varchar2 default 'N',
                p_prefetch in pls_integer default 1)
  is
    c_prefetch constant pls_integer := coalesce(p_prefetch, 1);
    l_payload_array te_payload_t;
  begin
    deq_int(p_qname, p_wait, p_payload, l_payload_array, p_payload_id, p_use_pipe, p_immediate, 1, c_prefetch);
  end;
  
  function deq_id(p_id in pls_integer default 1) return raw
  is
    c_id constant pls_integer := coalesce(p_id, 1);
  begin
    if g_deq_id_t.exists(c_id) then
      return g_deq_id_t(c_id);
    else
      return null;
    end if;
  end;
  
  /**
   * Функия контроля активности процессов и периода их запуска.
   * @param p_name Наименование процесса.
   * @param p_next_time Время следующего запуска.
   */
  function active(p_name in varchar2, 
                  p_next_time in out nocopy timestamp) return boolean
  is
    l_period number;
    l_time timestamp(6);
  begin
    loop
      l_time := systimestamp() at time zone 'utc';
      exit when p_next_time is null or l_time >= p_next_time;
      dbms_lock.sleep(greatest(seconds(p_next_time, l_time), 0) + 0.005); 
    end loop;
    begin
      select p.period
        into l_period
        from t_queue_process p
       where p.name = p_name
         and p.active = 'Y';
    exception
      when no_data_found then
        return false;
    end;
    p_next_time := coalesce(p_next_time, l_time) + numtodsinterval(l_period, 'second');
    return true;
  end;
  
  procedure monitor(p_qname in varchar2 default null)
  is
    type te_queue_s is record(id integer, name varchar2(100));
    type te_queue_list is table of te_queue_s;
    type te_data is record(qid integer, 
                           state integer, 
                           enq_time timestamp(6),
                           id raw(16),
                           expire_time timestamp(6),
                           deq_try integer,
                           deq_xid varchar2(30),
                           deq_time timestamp(6));
    type te_data_list is table of te_data;
    l_next_time timestamp(6);
    l_queue_list te_queue_list;
    l_queue_s te_queue_s;
    l_c sys_refcursor;
    l_data_list te_data_list;
    l_ready pls_integer;
    l_load_xid varchar2(1);
    l_time timestamp(6);
    l_prefetch_time timestamp(6);
    l_dummy varchar2(1);
  begin
    while active('monitor', l_next_time) loop
      if p_qname is null then
        select id, name bulk collect into l_queue_list from t_queue;
      else
        select id, name bulk collect into l_queue_list from t_queue where name = p_qname;
      end if;
      for q in 1 .. l_queue_list.count loop
        l_queue_s := l_queue_list(q);
        open l_c for 
          select /*+ index_asc(qd qeda_sepyee_idx) */
                 qd.qid, qd.state, qd.enq_time, qd.id, qd.expire_time, qd.deq_try, qd.deq_xid, qd.deq_time
            from t_queue_data qd
           where qd.qid = l_queue_s.id
             and qd.state = c_state_dequeued;
        l_ready := 0;
        loop
          fetch l_c bulk collect into l_data_list limit 128;
          exit when l_data_list.count = 0;
          if l_load_xid is null then
            load_xid_t; -- Загрузка активных транзакций.
            l_load_xid := 'Y';
          end if;
          for i in 1 .. l_data_list.count loop
            l_time := systimestamp() at time zone 'utc';
            l_prefetch_time := l_time - numtodsinterval(c_prefetch_period, 'second');
            if l_data_list(i).deq_xid is null then -- Предзагруженное сообщение.
              if l_data_list(i).deq_time < l_prefetch_time then -- Время резерва истекло.
                begin
                  select /*+ index(qd qeda_id_idx) */
                         null
                    into l_dummy
                    from t_queue_data qd 
                   where qd.qid = l_queue_s.id 
                     and qd.id = l_data_list(i).id
                     and qd.state = c_state_dequeued
                     and qd.deq_xid is null for update skip locked;
                  l_data_list(i).state := c_state_ready;
                  l_ready := l_ready + 1;
                exception
                  when no_data_found then
                    l_data_list.delete(i); -- Заблокировно (пропускаем).
                end;
              end if;
            elsif check_xid(l_data_list(i).deq_xid) = 'Y' then -- Транзакция активна.
              l_data_list.delete(i); -- Заблокировано (пропускаем).
            else
              init_queue(l_queue_s.name);
              if l_data_list(i).deq_try is null then
                l_data_list(i).deq_try := 1;
              else
                l_data_list(i).deq_try := l_data_list(i).deq_try + 1;
              end if;
              if l_data_list(i).expire_time < l_time or l_data_list(i).deq_try >= g_queue.try_count then
                l_data_list(i).qid := -l_queue_s.id;
                l_data_list(i).state := c_state_expired;
              else -- Повторная попытка.
                l_data_list(i).state := c_state_ready;
                l_data_list(i).enq_time := l_time;
                if g_queue.try_delay > 0 then
                  l_data_list(i).enq_time := l_data_list(i).enq_time + numtodsinterval(g_queue.try_delay, 'second');
                else
                  l_ready := l_ready + 1;
                end if;
              end if;
            end if;
          end loop;
          if l_data_list.count > 0 then
            forall i in indices of l_data_list
              update /*+ index(qd qeda_id_idx) */ 
                     t_queue_data qd
                 set qd.qid = l_data_list(i).qid,
                     qd.state = l_data_list(i).state,
                     qd.enq_time = l_data_list(i).enq_time,
                     qd.deq_try = l_data_list(i).deq_try
               where qd.qid = l_queue_s.id
                 and qd.id = l_data_list(i).id;
          end if;
          commit;
          ready_notify('Y', l_ready);
        end loop;
        close l_c;
      end loop;
    end loop;
  end;
  
  procedure maintenance
  is
    l_next_time timestamp(6);
    l_e varchar2(4000);
  begin
    while active('maintenance', l_next_time) loop
      for i in (select p.partition_name
                  from user_tab_partitions p
                 where p.table_name = c_data_table
                   and p.segment_created = 'YES')
      loop
        begin
          dbms_utility.exec_ddl_statement('alter index "' || c_schema || '"."' || c_data_index || '" ' || 
                                          'modify partition "' || i.partition_name || '" coalesce');
        exception
          when others then
            l_e := dbms_utility.format_error_stack;
        end;
      end loop;
      if not l_e is null then
        throw(14, l_e);
      end if;
    end loop;
  end;
  
end;
/
