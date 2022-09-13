
-module(zaya_leveldb).

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
    verify_compactions => false
    %compression => todo,
    %use_bloomfilter => todo,
    %total_memory => todo,
    %total_leveldb_mem => todo,
    %total_leveldb_mem_percent => todo,
    %is_internal_db => todo,
    %limited_developer_mem => todo,
    %eleveldb_threads => TODO pos_integer()
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
    sync => true
  }
}).

-define(DEFAULT_OPTIONS,#{
    dir => ".",
    open_attempts => ?DEFAULT_OPEN_ATTEMPTS ,
    eleveldb => ?DEFAULT_ELEVELDB_OPTIONS
}).
-define(env(K,D), application:get_env(zaya_leveldb,K,D)).


-define(OPTIONS(O),
  maps_merge(?DEFAULT_OPTIONS, O)
).

-define(EXT,"leveldb").
-define(LOCK(P),P++"/LOCK").

-define(DECODE_KEY(K), sext:decode(K) ).
-define(ENCODE_KEY(K), sext:encode(K) ).
-define(DECODE_VALUE(V), binary_to_term(V) ).
-define(ENCODE_VALUE(V), term_to_binary(V) ).

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.

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
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([

]).

-record(ref,{ref,read,write,dir}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( maps_merge(Params, #{eleveldb => #{open_options => #{ create_if_missing => true }}}) ),

  Path = Dir ++"."++?EXT,

  ensure_dir( Path ),

  Ref = try_open(Path, Options),
  close( Ref ),

  ok.

open( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( Params ),

  Path = Dir ++"."++?EXT,

  case filelib:is_dir( Path ) of
    true->
      ok;
    false->
      ?LOGERROR("~s doesn't exist",[ Path ]),
      throw(not_exists)
  end,

  try_open(Path, Options).

try_open(Path, #{
  eleveldb := #{
    open_options := Params,
    read := Read,
    write := Write
  },
  open_attempts := Attempts
} = Options) when Attempts > 0->

  ?LOGINFO("~s try open with params ~p",[Path, Params]),
  case eleveldb:open(Path, maps:to_list(Params)) of
    {ok, Ref} ->
      #ref{
        ref = Ref,
        read = maps:to_list(Read),
        write = maps:to_list(Write),
        dir = Path
      };
    %% Check for open errors
    {error, {db_open, Error}} ->
      % Check for hanging lock
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~s unable to open, hanging lock, trying to unlock",[Path]),
          case file:delete(?LOCK(Path)) of
            ok->
              ?LOGINFO("~s lock removed, trying open",[ Path ]),
              % Dont decrement the attempt because we fixed the error ourselves
              try_open(Path,Options);
            {error,UnlockError}->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually",[?LOCK(Path),UnlockError]),
              throw(locked)
          end;
        false ->
          ?LOGWARNING("~s open error ~p, try to repair left attempts ~p",[Path,Error,Attempts-1]),
          try eleveldb:repair(Path, [])
          catch
            _:E:S->
              ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attemps ~p",[Path,E,S,Attempts-1])
          end,
          try_open(Path,Options#{ open_attempts => Attempts -1 })
      end;
    {error, Other} ->
      ?LOGERROR("~s open error ~p, left attemps ~p",[Path,Other,Attempts-1]),
      try_open( Path, Options#{ open_attempts => Attempts -1 } )
  end;
try_open(Path, #{eleveldb := Params})->
  ?LOGERROR("~s OPEN ERROR: params ~p",[Path, Params]),
  throw(open_error).

close( #ref{ref = Ref} )->
  case eleveldb:close( Ref ) of
    ok -> ok;
    {error,Error}-> throw( Error)
  end.

remove( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( Params ),

  Path = Dir ++"."++?EXT,
  Attempts = ?DESTROY_ATTEMPTS,
  try_remove( Path, Attempts,  Options ).

try_remove( Path, Attempts, #{
  eleveldb := Params
} = Options) when Attempts >0 ->
  case eleveldb:destroy( Path, Params ) of
    ok->
      file:del_dir(Path),
      ?LOGINFO("~s removed",[Path]);
    {error, {error_db_destroy, Error}} ->
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~s unable to remove, hanging lock, trying to unlock",[ Path ]),
          case file:delete(?LOCK(Path)) of
            ok->
              ?LOGINFO("~s lock removed, trying to remove",[Path]),
              % Dont decrement the attempt because we fixed the error ourselves
              try_remove(Path,Attempts,Options);
            {error,UnlockError}->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually",[?LOCK(Path),UnlockError]),
              throw(locked)
          end;
        false ->
          ?LOGWARNING("~s remove error ~p, try to repair left attempts ~p",[Path,Error,Attempts-1]),
          try eleveldb:repair(Path, [])
          catch
            _:E:S->
              ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attemps ~p",[Path,E,S,Attempts-1])
          end,
          try_remove(Path, Attempts -1, Options)
      end;
    {error, Error} ->
      ?LOGWARNING("~s remove error ~p, try to repair left attempts ~p",[Path,Error,Attempts-1]),
      try eleveldb:repair(Path, [])
      catch
        _:E:S->
          ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attemps ~p",[Path,E,S,Attempts-1])
      end,
      try_remove(Path, Attempts -1, Options)
  end;
try_remove(Path, 0, #{eleveldb := Params})->
  ?LOGERROR("~s REMOVE ERROR: params ~p",[Path, Params]),
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
      {error, Error}->
        throw(Error)
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
      {error, Error}->
        throw(Error)
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
          {error,Error}->
            throw( Error )
        end;
      {ok,K,V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, Error}->
        throw(Error)
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

prev( #ref{ref = Ref, read = Params}, K )->
  Key = ?ENCODE_KEY(K),
  {ok, Itr} = eleveldb:iterator(Ref, [{first_key, Key}|Params]),
  try
    case eleveldb:iterator_move(Itr, prev) of
      {ok, K, V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, Error}->
        throw(Error)
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
      #{ stop:=Stop, ms:= MS }->
        StopKey = ?ENCODE_KEY(Stop),
        CompiledMS = ets:match_spec_compile(MS),
        iterate_query(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, StopKey, CompiledMS );
      #{ stop:= Stop }->
        iterate_stop(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ?ENCODE_KEY(Stop) );
      #{ms:= MS}->
        iterate_ms(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch, ets:match_spec_compile(MS) );
      _->
        iterate(eleveldb:iterator_move(Itr, StartKey), Itr, prefetch )
    end
  after
    catch eleveldb:iterator_close(Itr)
  end.

iterate_query({ok,K,V}, Itr, Next, StopKey, MS ) when K =< StopKey->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [_]->
      [Rec| iterate_query( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )];
    []->
      iterate_query( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )
  end;
iterate_query(_, _Itr, _Next, _StopKey, _MS )->
  [].

iterate_stop({ok,K,V}, Itr, Next, StopKey ) when K =< StopKey->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) }| iterate_stop( eleveldb:iterator_move(Itr,Next), Itr, Next, StopKey )];
iterate_stop(_, _Itr, _Next, _StopKey )->
  [].

iterate_ms({ok,K,V}, Itr, Next, MS )->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [_]->
      [Rec| iterate_ms( eleveldb:iterator_move(Itr,Next), Itr, Next, MS )];
    []->
      iterate_ms( eleveldb:iterator_move(Itr,Next), Itr, Next, MS )
  end;
iterate_ms(_, _Itr, _Next, _MS )->
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
            [_]->
              UserFun(Rec,Acc);
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
            [_]->
              UserFun(Rec,Acc);
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
  do_foldl( eleveldb:iterator_move(Itr,prev), Itr, Fun, Acc  );
do_foldr(_, _Itr, _Fun, Acc )->
  Acc.

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
%%	COPY
%%=================================================================
%%fold(#source{ref = Ref}, Iterator, Acc0)->
%%  eleveldb:fold(Ref,Iterator, Acc0, [{first_key, first}]).
%%
%%write_batch(Batch, CopyRef)->
%%  eleveldb:write(CopyRef,Batch, [{sync, true}]).
%%
%%drop_batch(Batch0,#source{ref = Ref})->
%%  Batch =
%%    [case R of {put,K,_}->{delete,K};_-> R end || R <- Batch0],
%%  eleveldb:write(Ref,Batch, [{sync, false}]).
%%
%%action({K,V})->
%%  {{put,K,V},size(K)+size(V)}.
%%
%%live_action({write, {K,V}})->
%%  K1 = ?ENCODE_KEY(K),
%%  {K1, {put, K1,?ENCODE_VALUE(V)} };
%%live_action({delete,K})->
%%  K1 = ?ENCODE_KEY(K),
%%  {K1,{delete,K1}}.
%%
%%get_key({put,K,_V})->K;
%%get_key({delete,K})->K.
%%
%%decode_key(K)->?DECODE_KEY(K).
%%
%%rollback_copy( Target )->
%%  todo.


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
      case file:delete( Path ) of
        ok->ok;
        {error,DelError}->
          ?LOGERROR("~s delete error ~p",[Path,DelError]),
          throw({delete_error,DelError})
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