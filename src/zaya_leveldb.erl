
-module(zaya_leveldb).

-include("zaya_leveldb.hrl").

-define(DEFAULT_OPEN_ATTEMPTS, 5).
-define(DESTROY_ATTEMPTS, 5).

-define(DEFAULT_ELEVELDB_OPTIONS,#{
  %compression_algorithm => todo,
  open_options=>#{
    %create_if_missing => false,
    %error_if_exists => false,
    %write_buffer_size => todo
    %sst_block_size => todo,
    %block_restart_interval = todo,
    %block_size_steps => todo,
    paranoid_checks => false,
    verify_compactions => false,
    compression => false,
    %use_bloomfilter => todo,
    %total_memory => todo,
    %total_leveldb_mem => todo,
    %total_leveldb_mem_percent => todo,
    %is_internal_db => todo,
    %limited_developer_mem => todo,
    eleveldb_threads => 64
    %fadvise_willneed => TODO boolean()
    %block_cache_threshold => TODO pos_integer()
    %delete_threshold => pos_integer()
    %tiered_slow_level => pos_integer()
    %tiered_fast_prefix => TODO string()
    %tiered_slow_prefix => TODO string()
    %cache_object_warming => TODO
    %expiry_enabled => TODO boolean()
    %expiry_minutes => TODO pos_integer()
    %whole_file_expiry => boolean()
  },
  read => #{
    verify_checksums => false
    %fill_cache => todo,
    %iterator_refresh =todo
  },
  write => #{
    sync => false
  }
}).

-define(DEFAULT_OPTIONS,#{
    dir => ".",
    pool => #{},
    open_attempts => ?DEFAULT_OPEN_ATTEMPTS ,
    eleveldb => ?DEFAULT_ELEVELDB_OPTIONS
}).
-define(env, maps_merge(?DEFAULT_OPTIONS, maps:from_list(application:get_all_env(zaya_leveldb))) ).


-define(OPTIONS(O),
  maps_merge(?env, O)
).

-define(LOCK(P),P++"/LOCK").
-define(RETRY_TIMEOUT,1000).

-define(DECODE_KEY(K), sext:decode(K) ).
-define(ENCODE_KEY(K), sext:encode(K) ).
-define(DECODE_VALUE(V), binary_to_term(V) ).
-define(ENCODE_VALUE(V), term_to_binary(V) ).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

%%=================================================================
%%	POOL API
%%=================================================================
-export([
  pool_batch/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{
  ref,
  read,
  write,
  dir,
  pool
}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  Options = #{
    dir := Dir,
    eleveldb := #{
      read := Read,
      write := Write
    }
  } = ?OPTIONS(maps_merge(
      Params,
      #{
        eleveldb => #{
          open_options => #{
            create_if_missing => true
          }
        }
      }
    )),

  ensure_dir( Dir ),

  InitRef = #ref{
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  },
  case pipe(InitRef, [
    fun(Ref)-> try_open(Ref, Options) end,
    fun(Ref)-> open_pool(Ref, Options) end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      catch remove(Params),
      throw(Error)
  end.

open( Params )->
  Options = #{
    dir := Dir,
    eleveldb := #{
      read := Read,
      write := Write
    }
  } = ?OPTIONS(maps_merge(
    Params,
    #{
      eleveldb => #{
        open_options => #{
          create_if_missing => false
        }
      }
    }
  )),

  case filelib:is_dir( Dir ) of
    true->
      ok;
    false->
      ?LOGERROR("~s doesn't exist",[ Dir ]),
      throw(not_exists)
  end,

  InitRef = #ref{
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  },
  case pipe(InitRef, [
    fun(Ref)-> try_open(Ref, Options) end,
    fun(Ref)-> open_pool(Ref, Options) end
  ]) of
    {ok, Ref}-> Ref;
    {error, Error, Ref}->
      catch close(Ref),
      throw(Error)
  end.

try_open(Ref = #ref{dir = Dir}, #{
  eleveldb := #{
    open_options := Params
  },
  open_attempts := Attempts
} = Options) when Attempts > 0->

  ?LOGINFO("~s try open with params ~p",[Dir, Params]),
  case eleveldb:open(Dir, maps:to_list(Params)) of
    {ok, DBRef} ->
      Ref#ref{ref = DBRef};
    %% Check for open errors
    {error, {db_open, Error}} ->
      % Check for hanging lock
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~s unable to open, hanging lock, trying to unlock",[Dir]),
          case file:delete(?LOCK(Dir)) of
            ok->
              ?LOGINFO("~s lock removed, trying open",[ Dir ]),
              timer:sleep( ?RETRY_TIMEOUT ),
              % Dont decrement the attempt because we fixed the error ourselves
              try_open(Ref, Options);
            {error,UnlockError}->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually",[?LOCK(Dir),UnlockError]),
              throw(locked)
          end;
        false ->
          ?LOGWARNING("~s open error ~p, try to repair left attempts ~p",[Dir,Error,Attempts-1]),
          try eleveldb:repair(Dir, [])
          catch
            _:E:S->
              ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attemps ~p",[Dir,E,S,Attempts-1])
          end,
          timer:sleep( ?RETRY_TIMEOUT ),
          try_open(Ref,Options#{ open_attempts => Attempts -1 })
      end;
    {error, Other} ->
      ?LOGERROR("~s open error ~p, left attemps ~p",[Dir,Other,Attempts-1]),
      timer:sleep( ?RETRY_TIMEOUT ),
      try_open(Ref, Options#{ open_attempts => Attempts -1 } )
  end;
try_open(#ref{dir = Dir}, #{eleveldb := Params})->
  ?LOGERROR("~s OPEN ERROR: params ~p",[Dir, Params]),
  throw(open_error).

close( #ref{ref = Ref, pool = Pool} )->
  catch close_pool(Pool),
  catch eleveldb:close( Ref ),
  ok.

remove( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( Params ),

  Attempts = ?DESTROY_ATTEMPTS,
  try_remove( Dir, Attempts,  Options ).

try_remove( Dir, Attempts, Options) when Attempts >0 ->
  try remove_recursive(Dir)
  catch
    _:E:S->
      ?LOGERROR("~s remove error ~p stack ~p",[Dir,E,S]),
      try_remove( Dir, Attempts -1, Options )
  end;
try_remove(Dir, 0, #{eleveldb := Params})->
  ?LOGERROR("~s REMOVE ERROR: params ~p",[Dir, Params]),
  throw(remove_error).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ref = Ref, read = Params}=R, [Key|Rest])->
  case eleveldb:get(Ref, ?ENCODE_KEY(Key), Params) of
    {ok, Value} ->
      [{Key,?DECODE_VALUE(Value)} | read(R,Rest) ];
    _->
      read(R, Rest)
  end;
read(_R,[])->
  [].

write(#ref{ref = Ref, write = Params, pool = disabled}, KVs)->
  Ops = prepare_write(KVs),
  case eleveldb:write(Ref, Ops, Params) of
    ok->ok;
    {error,Error}->throw(Error)
  end;
write(#ref{pool = Pool}, KVs)->
  Ops = prepare_write(KVs),
  zaya_pool:call(Pool, [{batch, Ops}]).

prepare_write(KVs)->
  [{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} || {K,V} <- KVs].

delete(#ref{ref = Ref, write = Params, pool = disabled},Keys)->
  Ops = prepare_delete(Keys),
  case eleveldb:write(Ref, Ops, Params) of
    ok -> ok;
    {error, Error}-> throw(Error)
  end;
delete(#ref{pool = Pool}, Keys)->
  Ops = prepare_delete(Keys),
  zaya_pool:call(Pool, [{batch, Ops}]).

prepare_delete(Keys)->
  [{delete,?ENCODE_KEY(K)} || K <- Keys].

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ref = Ref, read = Params} )->
  {ok, Itr} = eleveldb:iterator(Ref, Params),
  try
    case eleveldb:iterator_move(Itr, first) of
      {ok, K, V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

last( #ref{ref = Ref, read = Params} )->
  {ok, Itr} = eleveldb:iterator(Ref, Params),
  try
    case eleveldb:iterator_move(Itr, last) of
      {ok, K, V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

next( #ref{ref = Ref, read = Params}, K0 )->
  Key = ?ENCODE_KEY(K0),
  {ok, Itr} = eleveldb:iterator(Ref, Params),
  try
    case eleveldb:iterator_move(Itr, Key) of
      {ok, Key, _}->
        case eleveldb:iterator_move( Itr, next ) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end;
      {ok,K,V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

prev( #ref{ref = Ref, read = Params}, K0 )->
  Key = ?ENCODE_KEY(K0),
  {ok, Itr} = eleveldb:iterator(Ref, Params),
  try
    case eleveldb:iterator_move(Itr, Key) of
      {ok,_,_}->
        case eleveldb:iterator_move( Itr, prev ) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end;
      {error, _}->
        case eleveldb:iterator_move(Itr, last) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ref = Ref, read = Params}, Query)->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->first
    end,

  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop, ms:= MS, limit:=Limit }->
        StopKey = ?ENCODE_KEY(Stop),
        CompiledMS = ets:match_spec_compile(MS),
        iterate_query(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, StopKey, CompiledMS, Limit );
      #{ stop:=Stop, ms:= MS}->
        StopKey = ?ENCODE_KEY(Stop),
        CompiledMS = ets:match_spec_compile(MS),
        iterate_ms_stop(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, StopKey, CompiledMS );
      #{ stop:= Stop, limit := Limit }->
        iterate_stop_limit(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ?ENCODE_KEY(Stop), Limit );
      #{ stop:= Stop }->
        iterate_stop(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ?ENCODE_KEY(Stop) );
      #{ms:= MS, limit := Limit}->
        iterate_ms_limit(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ets:match_spec_compile(MS), Limit );
      #{ms:= MS}->
        iterate_ms(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ets:match_spec_compile(MS) );
      #{limit := Limit}->
        iterate_limit(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, Limit );
      _->
        iterate(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch )
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

iterate_query({ok,K,V}, Itr, Next, StopKey, MS, Limit ) when K =< StopKey, Limit > 0->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_query( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS, Limit - 1 )];
    []->
      iterate_query( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS, Limit )
  end;
iterate_query(_, _Itr, _Next, _StopKey, _MS, _Limit )->
  [].

iterate_ms_stop({ok,K,V}, Itr, Next, StopKey, MS ) when K =< StopKey->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_stop( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )];
    []->
      iterate_ms_stop( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )
  end;
iterate_ms_stop(_, _Itr, _Next, _StopKey, _MS )->
  [].

iterate_stop_limit({ok,K,V}, Itr, Next, StopKey, Limit ) when K =< StopKey, Limit > 0->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) }| iterate_stop_limit( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, Limit -1 )];
iterate_stop_limit(_, _Itr, _Next, _StopKey, _Limit )->
  [].

iterate_stop({ok,K,V}, Itr, Next, StopKey ) when K =< StopKey->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) }| iterate_stop( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey )];
iterate_stop(_, _Itr, _Next, _StopKey )->
  [].

iterate_ms_limit({ok,K,V}, Itr, Next, MS, Limit ) when Limit >0->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_limit( eleveldb:iterator_move(Itr,Next), Itr, Next, MS, Limit - 1 )];
    []->
      iterate_ms_limit( eleveldb:iterator_move(Itr,Next), Itr, Next, MS, Limit )
  end;
iterate_ms_limit(_, _Itr, _Next, _MS, _Limit )->
  [].

iterate_ms({ok,K,V}, Itr, Next, MS )->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms( eleveldb:iterator_move(Itr,Next), Itr, Next, MS )];
    []->
      iterate_ms( eleveldb:iterator_move(Itr,Next), Itr, Next, MS )
  end;
iterate_ms(_, _Itr, _Next, _MS )->
  [].

iterate_limit({ok,K,V}, Itr, Next, Limit) when Limit >0->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) } | iterate_limit( eleveldb:iterator_move(Itr,Next), Itr, Next, Limit-1 ) ];
iterate_limit(_, _Itr, _Next, _Limit )->
  [].

iterate({ok,K,V}, Itr, Next)->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) } | iterate( eleveldb:iterator_move(Itr,Next), Itr, Next ) ];
iterate(_, _Itr, _Next )->
  [].

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->first
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldl_stop( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldl( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}->Acc
  after
    catch eleveldb:iterator_close(Itr)
  end.

do_foldl_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K =< StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl_stop( eleveldb:iterator_move(Itr,prefetch), Itr, Fun, Acc, StopKey  );
do_foldl_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldl( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl( eleveldb:iterator_move(Itr,prefetch), Itr, Fun, Acc  );
do_foldl(_, _Itr, _Fun, Acc )->
  Acc.

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->last
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldr_stop( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldr( eleveldb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}-> Acc
  after
    catch eleveldb:iterator_close(Itr)
  end.

do_foldr_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K >= StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldr_stop( eleveldb:iterator_move(Itr,prev), Itr, Fun, Acc, StopKey  );
do_foldr_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldr( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldr( eleveldb:iterator_move(Itr,prev), Itr, Fun, Acc  );
do_foldr(_, _Itr, _Fun, Acc )->
  Acc.

%%=================================================================
%%	COPY
%%=================================================================
copy(#ref{ref = Ref, read = Params}, Fun, InAcc)->
  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, first}|Params]),
  try
    do_copy( eleveldb:iterator_move(Itr, first), Itr, Fun, InAcc )
  catch
    {stop,Acc}->Acc
  after
    catch eleveldb:iterator_close(Itr)
  end.

%% TODO. We need to release the iterator between batches because it can hog the memory
do_copy( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {K, V}, InAcc ),
  do_copy( eleveldb:iterator_move(Itr,prefetch), Itr, Fun, Acc  );
do_copy(_, _Itr, _Fun, Acc )->
  Acc.

dump_batch(#ref{ref = Ref, write = Params}, KVs)->
  case eleveldb:write(Ref,[{put,K,V} || {K,V} <- KVs ], Params) of
    ok->ok;
    {error,Error}->throw(Error)
  end.

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit( #ref{ ref = DRef, write = Params, pool = disabled}, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  ok = eleveldb:write( DRef, Commit, Params);
commit( #ref{ pool = Pool}, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  zaya_pool:call(Pool, [{batch, Commit}]).

prepare_commit([{K,V}|Rest], Delete )->
  [{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} | prepare_commit(Rest, Delete) ];
prepare_commit([], [K|Rest] )->
  [{delete,?ENCODE_KEY(K)} | prepare_commit([], Rest) ];
prepare_commit([], [])->
  [].

prepare_rollback(Ref, Write, Delete)->
  {W_acc0, D_acc} = rollback_write(Write, Ref, {[], []}),
  W_acc = rollback_delete(Delete, Ref, W_acc0),
  {W_acc, D_acc}.

rollback_write(
    [{K,V}|Rest],
    Ref = #ref{ref = DBRef, read = Params},
    Acc0 = {W_acc,D_acc}
)->
  Acc =
    case eleveldb:get(DBRef, ?ENCODE_KEY(K), Params) of
      {ok, V_enc}->
        case ?DECODE_VALUE(V_enc) of
          V -> Acc0;
          V0-> {[{K,V0}|W_acc], D_acc}
        end;
      _->
        {W_acc, [K|D_acc]}
    end,
  rollback_write(Rest, Ref, Acc);
rollback_write([], _Ref, Acc)->
  Acc.

rollback_delete(
    [K|Rest],
    Ref = #ref{ref = DBRef, read = Params},
    Acc0
)->
  Acc =
    case eleveldb:get(DBRef, ?ENCODE_KEY(K), Params) of
      {ok, V} -> [{K, ?DECODE_VALUE(V)}|Acc0];
      _-> Acc0
    end,
  rollback_delete(Rest, Ref, Acc);
rollback_delete([], _Ref, Acc)->
  Acc.

is_persistent()->
  true.

%%=================================================================
%%	POOL API
%%=================================================================
pool_batch({DRef, WriteParams}, Requests)->
  Ops = lists:append([Batch || {batch, Batch} <- Requests]),
  case eleveldb:write(DRef, Ops, WriteParams) of
    ok -> ok;
    {error, Error} -> throw(Error)
  end.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  get_size( Ref, 10 ).
get_size( #ref{dir = Dir} = R, Attempts ) when Attempts > 0->
  S = list_to_binary(os:cmd("du -s --block-size=1 "++ Dir)),
  case binary:split(S,<<"\t">>) of
    [Size|_]->
      try binary_to_integer( Size )
      catch _:_->
        % Sometimes du returns error when there are some file transformations
        timer:sleep(200),
        get_size( R, Attempts - 1 )
      end;
    _ ->
      timer:sleep(200),
      get_size( R, Attempts - 1 )
  end;
get_size( _R, 0 )->
  -1.

%%=================================================================
%%	UTIL
%%=================================================================
maps_merge( Map1, Map2 )->
  maps:fold(fun(K,V2,Acc)->
    case Map1 of
      #{K := V1} when is_map(V1),is_map(V2)->
        Acc#{ K => maps_merge( V1,V2 ) };
      _->
        Acc#{ K => V2 }
    end
  end, Map1, Map2 ).


%%=================================================================
%%	UTILITIES
%%=================================================================
pipe(Ref, [Step | Rest])->
  try Step(Ref) of
    Ref1 -> pipe(Ref1, Rest)
  catch _:Error ->
    {error, Error, Ref}
  end;
pipe(Ref, [])->
  {ok, Ref}.

open_pool(Ref, #{pool := disabled})->
  Ref#ref{pool = disabled};
open_pool(Ref = #ref{ref = DRef, write = WriteParams}, Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(DRef, WriteParams, Params)),
  Ref#ref{pool = Pool}.

close_pool(undefined)->
  ok;
close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

pool_params(DRef, WriteParams, Params)->
  maps:merge(
    maps:get(pool, Params, #{}),
    #{
      ref => {DRef, WriteParams},
      module => ?MODULE
    }
  ).

ensure_dir( Path )->
  case filelib:is_file( Path ) of
    false->
      case filelib:ensure_dir( Path++"/" ) of
        ok -> ok;
        {error,CreateError}->
          ?LOGERROR("~s create error ~p",[ Path, CreateError ]),
          throw({create_dir_error,CreateError})
      end;
    true->
      remove_recursive( Path ),
      ensure_dir( Path )
  end.

remove_recursive( Path )->
  case filelib:is_dir( Path ) of
    false->
      case filelib:is_file( Path ) of
        true->
          case file:delete( Path ) of
            ok->ok;
            {error,DelError}->
              ?LOGERROR("~s delete error ~p",[Path,DelError]),
              throw({delete_error,DelError})
          end;
        _->
          ok
      end;
    true->
      case file:list_dir_all( Path ) of
        {ok,Files}->
          [ remove_recursive(Path++"/"++F) || F <- Files ],
          case file:del_dir( Path ) of
            ok->
              ok;
            {error,DelError}->
              ?LOGERROR("~s delete error ~p",[Path,DelError]),
              throw({delete_error,DelError})
          end
      end
  end.
