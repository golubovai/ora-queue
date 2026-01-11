create sequence seq_qid;
create table t_queue(id integer,
                     name varchar2(100),
                     try_count integer,
                     try_delay number,
                     low_latency varchar2(1),
                     enqueue varchar2(1),
                     dequeue varchar2(1));
alter table t_queue add check (id is not null);
alter table t_queue add check (name is not null);
alter table t_queue add check (try_count is not null and try_count > 0);
alter table t_queue add check (try_delay is not null and try_delay >= 0);
alter table t_queue add check (low_latency is not null and low_latency in ('Y', 'N'));
alter table t_queue add check (enqueue is not null and enqueue in ('Y', 'N'));
alter table t_queue add check (dequeue is not null and dequeue in ('Y', 'N'));
create unique index qe_unq on t_queue(id);
create unique index qe_ne_unq on t_queue(name);
comment on table t_queue is 'Очередь';
comment on column t_queue.id is 'Идентификатор очереди';
comment on column t_queue.name is 'Наименование очереди';
comment on column t_queue.try_count is 'Число попыток обработки';
comment on column t_queue.try_delay is 'Задержка повторной попытки обработки';
comment on column t_queue.low_latency is 'Минимальная задержка извлечения';
comment on column t_queue.enqueue is 'Отправка включена';
comment on column t_queue.dequeue is 'Извлечение включено';


create table t_queue_data(qid integer,
                          state integer,
                          priority integer default 1,
                          enq_time timestamp(6),
                          id raw(16),
                          payload blob,
                          expire_time timestamp(6),
                          deq_try integer,
                          deq_xid varchar2(30),
                          deq_time timestamp(6)) partition by list(qid) automatic (partition empty_lp values (null)) enable row movement;
alter table t_queue_data add check (qid is not null);
alter table t_queue_data add check (state is not null);
alter table t_queue_data add check (priority is not null);
alter table t_queue_data add check (enq_time is not null);
alter table t_queue_data add check (id is not null);
create index qeda_id_idx on t_queue_data(id) local;
create index qeda_sepyee_idx on t_queue_data(state, priority, enq_time) local pctfree 0;
comment on table t_queue_data is 'Данные сообщения очереди';
comment on column t_queue_data.qid is 'Идентификатор очереди';
comment on column t_queue_data.state is 'Состояние сообщения';
comment on column t_queue_data.priority is 'Приоритет сообщения';
comment on column t_queue_data.enq_time is 'Время доступности сообщения';
comment on column t_queue_data.id is 'Идентификатор сообщения';
comment on column t_queue_data.payload is 'Данные сообщения';
comment on column t_queue_data.expire_time is 'Время жизни сообщения';
comment on column t_queue_data.deq_try is 'Извлечений сообщения произведено';
comment on column t_queue_data.deq_xid is 'Идентификатор транзакции извлечения сообщения';
comment on column t_queue_data.deq_time is 'Время извлечения сообщения';


create table t_queue_process(name varchar2(128), period number, active varchar2(1));
alter table t_queue_process add check (name is not null);
alter table t_queue_process add check (period is not null);
alter table t_queue_process add check (active is not null and active in ('Y', 'N'));
create unique index qeps_ne_unq on t_queue_process(name);
comment on table t_queue_process is 'Служебный процесс очереди';
comment on column t_queue_process.name is 'Наименование процесса';
comment on column t_queue_process.period is 'Период работы';
comment on column t_queue_process.active is 'Активен';

insert into t_queue_process values ('monitor', 1, 'Y');
insert into t_queue_process values ('maintenance', 20, 'Y');
commit;

@@p_queue.pck

-- Заводит задания мониторинга и обслуживания.
begin
  dbms_scheduler.create_job('QMONITOR', 'PLSQL_BLOCK', 'p_queue.monitor;', start_date => systimestamp(), repeat_interval => 'freq=minutely;', enabled => true);
  dbms_scheduler.create_job('QMAINTENANCE', 'PLSQL_BLOCK', 'p_queue.maintenance;', start_date => systimestamp(), repeat_interval => 'freq=minutely;', enabled => true);
end;
/
