
-module(zaya_leveldb).

-include("zaya_leveldb.hrl").

-define(DEFAULT_OPEN_ATTEMPTS, 5).
-define(DESTROY_ATTEMPTS, 5).

-define(DEFAULT_ELEVELDB_OPTIONS,#{
  %compression_algorithm => todo,
  open_options=>#{
    create_if_missing => false,
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

-define(ROLLBACK_KEY( Ref ),?ENCODE_KEY({rollback, ?MODULE, TRef})).
-define(none, {?MODULE, undefined}).


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
  commit1/3,
  commit2/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ref,log,read,write,dir}).

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
  } = ?OPTIONS( maps_merge(Params, #{eleveldb => #{open_options => #{ create_if_missing => true }}}) ),

  DataDir = Dir ++"/DATA",
  LogDir = Dir++"/LOG",

  ensure_dir( DataDir ),
  ensure_dir( LogDir ),

  #ref{
    ref = try_open(DataDir, Options),
    log = try_open(LogDir, Options),
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  }.

open( Params )->
  Options = #{
    dir := Dir,
    eleveldb := #{
      read := Read,
      write := Write
    }
  } = ?OPTIONS( Params ),

  DataDir = Dir ++"/DATA",
  LogDir = Dir++"/LOG",

  case filelib:is_dir( DataDir ) of
    true->
      case filelib:is_dir( LogDir ) of
        true -> ok;
        false ->
          ?LOGERROR("~s doesn't exist",[ LogDir ]),
          throw(not_exists)
      end,
      ok;
    false->
      ?LOGERROR("~s doesn't exist",[ DataDir ]),
      throw(not_exists)
  end,

  Ref = #ref{
    ref = try_open(DataDir, Options),
    log = try_open(LogDir, Options),
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  },

  rollback_log( Ref ),

  Ref.

try_open(Dir, #{
  eleveldb := #{
    open_options := Params
  },
  open_attempts := Attempts
} = Options) when Attempts > 0->

  ?LOGINFO("~s try open with params ~p",[Dir, Params]),
  case eleveldb:open(Dir, maps:to_list(Params)) of
    {ok, Ref} ->
      Ref;
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
              try_open(Dir,Options);
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
          try_open(Dir,Options#{ open_attempts => Attempts -1 })
      end;
    {error, Other} ->
      ?LOGERROR("~s open error ~p, left attemps ~p",[Dir,Other,Attempts-1]),
      timer:sleep( ?RETRY_TIMEOUT ),
      try_open( Dir, Options#{ open_attempts => Attempts -1 } )
  end;
try_open(Dir, #{eleveldb := Params})->
  ?LOGERROR("~s OPEN ERROR: params ~p",[Dir, Params]),
  throw(open_error).

close( #ref{ref = Ref, log = Log} )->
  case eleveldb:close( Ref ) of
    ok ->
      case eleveldb:close( Log ) of
        ok -> ok;
        {error, Error} -> throw( Error)
      end;
    {error,Error}->
      throw( Error)
  end.

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

write(#ref{ref = Ref, write = Params}, KVs)->
  case eleveldb:write(Ref,[{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} || {K,V} <- KVs ], Params) of
    ok->ok;
    {error,Error}->throw(Error)
  end.

delete(#ref{ref = Ref, write = Params},Keys)->
  case eleveldb:write(Ref,[{delete,?ENCODE_KEY(K)} || K <- Keys], Params) of
    ok -> ok;
    {error, Error}-> throw(Error)
  end.

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
%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit( #ref{ ref = DRef,write = Params}, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  ok = eleveldb:write( DRef, Commit, Params).

commit1( #ref{ ref = DRef,log = Log ,write = Params} = Ref, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  Rollback = prepare_rollback( Commit , Ref ),
  if
    length( Rollback ) =:=0-> ignore;
    true ->
      TRef = ?ENCODE_KEY( make_ref() ),
      try
        ok = eleveldb:write( Log, [ {put,TRef, ?ENCODE_VALUE(Rollback) }], Params),
        ok = eleveldb:write( DRef, Commit, Params),
        TRef
      catch
        _:E->
          rollback( Ref, TRef ),
          throw( E )
      end
  end.

commit2( #ref{log = Log, write = Params} , TRef )->
  if
    TRef =/= ignore ->
      ok = eleveldb:write(Log, [{delete,TRef}], Params);
    true ->
      ok
  end.

rollback(#ref{ref =Ref, log = Log ,write = Params}, TRef )->
  case eleveldb:get(Log, TRef, Params) of
    {ok, Rollback} ->
      ok = eleveldb:write( Ref, ?DECODE_VALUE( Rollback ), Params ),
      ok = eleveldb:write( Log, [{delete,TRef}], Params);
    _->
      ok
  end.

prepare_commit([{K,V}|Rest], Delete )->
  [{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} | prepare_commit(Rest, Delete) ];
prepare_commit([], [K|Rest] )->
  [{delete,?ENCODE_KEY(K)} | prepare_commit([], Rest) ];
prepare_commit([], [])->
  [].

prepare_rollback([{put,K,V}|Rest], #ref{ ref = DRef ,read = Params} = Ref)->
  case eleveldb:get(DRef, K, Params) of
    {ok, V} ->
      prepare_rollback(Rest, Ref);
    {ok, V0} ->
      [{put, K, V0} | prepare_rollback(Rest, Ref) ];
    _->
      [{delete, K} | prepare_rollback(Rest, Ref) ]
  end;

prepare_rollback([{delete, K}|Rest], #ref{ ref = DRef ,read = Params} = Ref)->
  case eleveldb:get(DRef, K, Params) of
    {ok, V} ->
      [{put, K, V} | prepare_rollback(Rest, Ref) ];
    _->
      prepare_rollback(Rest, Ref)
  end;
prepare_rollback([], _Ref)->
  [].

rollback_log( #ref{ ref = Ref, log = Log, read = ReadParams, write = WriteParams } )->
  eleveldb:fold(Log, fun({TRef, Rollback}, _Acc)->
    eleveldb:write( Ref, ?DECODE_VALUE( Rollback ), WriteParams ),
    eleveldb:write(Log, [{delete,TRef}], WriteParams)
  end, ok, ReadParams).


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