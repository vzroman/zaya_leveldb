
-module(zaya_leveldb).

-define(DEFAULT_ELEVELDB_OPTIONS,#{
  
}).
-define(EXT,"leveldb").
-define(LOCK(P),P++"/LOCK").

-define(REF(T),mnesia_eleveldb:get_ref(T)).
-define(DECODE_KEY(K),mnesia_eleveldb:decode_key(K)).
-define(ENCODE_KEY(K),mnesia_eleveldb:encode_key(K)).
-define(DECODE_VALUE(V),element(3,mnesia_eleveldb:decode_val(V))).
-define(ENCODE_VALUE(V),mnesia_eleveldb:encode_val({[],[],V})).

-define(MOVE(I,K),eleveldb:iterator_move(I,K)).
-define(NEXT(I),eleveldb:iterator_move(I,next)).
-define(PREV(I),eleveldb:iterator_move(I,prev)).

% The encoded @deleted@ value. Actually this is {[],[],'@deleted@'}
-define(DELETED, <<131,104,3,106,106,100,0,9,64,100,101,108,101,116,101,100,64>>).
-define(MAX_SEARCH_SIZE,1 bsl 128).

%%=================================================================
%%	STORAGE SERVER API
%%=================================================================
-export([
  ext/0,
  create/2,
  open/2,
  close/1
]).

%%=================================================================
%%	GET/PUT API
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
%%	SEARCH API
%%=================================================================
-export([
  search/2,
  match/2
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
  init_source/2,
  init_target/2,
  init_copy/1,

  dump_source/1,
  dump_target/1,
  rollback_copy/1,

  fold/3,

  write_batch/2,
  drop_batch/2,

  get_key/1,
  decode_key/1,

  action/1,
  live_action/1
]).

-define(DEFAULT_PARAMS,#{
  options => ?DEFAULT_ELEVELDB_OPTIONS,
  attempts => 5
}).
-define(PARAMS(P),maps:merge(?DEFAULT_PARAMS,P)).

%%=================================================================
%%	STORAGE SERVER
%%=================================================================
ext()->
  ?EXT.

check_params(Params)->
  todo.

create( Segment, #{
  path := Path
})->
  SPath = ?S_PATH(Path,Segment,?EXT_ELEVELDB),

  case filelib:is_dir(SPath) of
    false->
      case filelib:is_file(SPath) of
        false->ok;
        true->
          ?LOGERROR("~p creaate error ~p already exists, try to remove it first",[Segment,SPath]),
          throw(not_empty)
      end;
    true->
      ?LOGERROR("~p create error ~p already exists, try to remove it first",[Segment,SPath]),
      throw(not_empty)
  end,

  case filelib:ensure_dir( SPath ) of
    ok->ok;
    {error, Error}->
      ?LOGERROR("~p create error ~p unable create error ~p",[Segment,SPath,Error]),
      throw(unavailable_path)
  end,

  ok.

open( Segment, Params0 )->
  Params = #{
    path:=Path
  } = ?PARAMS( Params0 ),

  SPath = ?S_PATH(Path,Segment,?EXT_ELEVELDB),

  case filelib:is_dir(SPath) of
    false->
      ?LOGERROR("~p doesn't exist"),
      throw(not_exists);
    true->
      ok
  end,

  try_open(Segment, SPath, Params).

try_open(Segment, Path, #{
  options := Options,
  attempts := Attempts
} = Params ) when Attempts > 0->

  ?LOGINFO("~p path ~p try open with options ~p",[Segment,Path,Options]),
  case eleveldb:open(Path, Options) of
    {ok, Ref} ->
      ?LOGINFO("~p is ready ref ~p",[Segment,Ref]),
      {ok, Ref};
    %% Check for open errors
    {error, {db_open, Error}} ->
      % Check for hanging lock
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~p unable to open, hanging lock, try to remove"),
          case file:delete(?LOCK(Path)) of
            ok->
              ?LOGINFO("~p lock removed, retry"),
              % Dont decrement the attempt because we fixed the error ourselves
              try_open(Segment,Path,Params)
          end;
        {error,UnlockError}->
          ?LOGERROR("~p remove lock ~p error ~p, try to remove it first",[Segment,?LOCK(Path),UnlockError]),
          throw(locked);
        false ->
          ?LOGWARNING("~p open error ~p, try to repair left attempts ~p",[Segment,Error,Attempts-1]),
          try eleveldb:repair(Path, [])
          catch
            _:E:S->
              ?LOGWARNING("~p repair attempt failed error ~p stack ~p, left attemps ~p",[Segment,E,S,Attempts-1]),
              try_open(Segment,Path,Params#{ attempts => Attempts -1 })
          end
      end;
    {error, Other} ->
      ?LOGERROR("~p open error ~p, left attemps ~p",[Segment,Other,Attempts-1]),
      try_open( Segment, Path, Params#{ attempts => Attempts -1 } )
  end;
try_open(Segment,Path,Params)->
  ?LOGERROR("~p OPEN ERROR: path ~p params ~p",[Segment,Path,Params]),
  throw(open_error).

close( Ref )->
  todo.


%%=================================================================
%%	READ/WRITE
%%=================================================================
read(Segment, Keys)->
  ok.

write(Segment,Records)->

  eleveldb:write(Ref,Batch, [{sync, Sync}]),
  ok.

delete(Segment,Keys)->
  Batch =
    [case R of {put,K,_}->{delete,K};_-> R end || R <- Batch0],
  eleveldb:write(Ref,Batch, [{sync, false}]),
  ok.
%%=================================================================
%%	ITERATOR
%%=================================================================
first( Segment )->
  ok.

last( Segment )->
  ok.

next( Segment, K )->
  ok.

prev( Segment, K )->
  ok.

%%=================================================================
%%	SEARCH
%%=================================================================
search(Segment,#{
  start := Start,
  stop := Stop,
  ms := MS
})->
  ok.

match( Segment, Pattern )->
  ok.

%----------------------DISC SCAN ALL TABLE, NO LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table','$end_of_table',Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN ALL TABLE, NO LIMIT-------------"),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?DATA_START),I) end);

%----------------------DISC SCAN ALL TABLE, WITH LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table','$end_of_table',Limit)->
  ?LOGDEBUG("------------DISC SCAN ALL TABLE, LIMIT ~p-------------",[Limit]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?DATA_START),I,Limit) end);

%----------------------DISC SCAN FROM START TO KEY, NO LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table',To,Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM START TO KEY ~p, NO LIMIT-------------",[To]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?DATA_START),I,?ENCODE_KEY(To)) end);

%----------------------DISC SCAN FROM START TO KEY, WITH LIMIT-----------------------------------------
disc_scan(Segment,'$start_of_table',To,Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM START TO KEY ~p, WITH LIMIT ~p-------------",[To,Limit]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?DATA_START),I,?ENCODE_KEY(To),Limit) end);

%----------------------DISC SCAN FROM KEY TO END, NO LIMIT-----------------------------------------
disc_scan(Segment,From,'$end_of_table',Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO END, NO LIMIT-------------",[From]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?ENCODE_KEY(From)),I) end);

%----------------------DISC SCAN FROM KEY TO END, WITH LIMIT-----------------------------------------
disc_scan(Segment,From,'$end_of_table',Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO END, WITH LIMIT ~p-------------",[From,Limit]),
  fold(Segment,fun(I)-> do_fold(?MOVE(I,?ENCODE_KEY(From)),I,Limit) end);

%----------------------DISC SCAN FROM KEY TO KEY, NO LIMIT-----------------------------------------
disc_scan(Segment,From,To,Limit)
  when not is_number(Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO ~p, NO LIMIT-------------",[From,To]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?ENCODE_KEY(From)),I,?ENCODE_KEY(To)) end);

%----------------------DISC SCAN FROM KEY TO KEY, WITH LIMIT-----------------------------------------
disc_scan(Segment,From,To,Limit)->
  ?LOGDEBUG("------------DISC SCAN FROM ~p TO ~p, WITH LIMIT ~p-------------",[From,To,Limit]),
  fold(Segment,fun(I)-> do_fold_to(?MOVE(I,?ENCODE_KEY(From)),I,?ENCODE_KEY(To),Limit) end).

fold(Segment,Fold) ->
  Ref = ?REF(Segment),
  {ok, Itr} = eleveldb:iterator(Ref, []),
  try Fold(Itr)
  after
    catch eleveldb:iterator_close(Itr)
  end.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Table)->
  get_size( Table, 10 ).
get_size( Table, Attempts ) when Attempts > 0->
  MP = mnesia_eleveldb:data_mountpoint( Table ),
  S = list_to_binary(os:cmd("du -s --block-size=1 "++MP)),
  case binary:split(S,<<"\t">>) of
    [Size|_]->
      try binary_to_integer( Size )
      catch _:_->
        % Sometimes du returns error when there are some file transformations
        timer:sleep(200),
        get_size( Table, Attempts - 1 )
      end;
    _ ->
      timer:sleep(200),
      get_size( Table, Attempts - 1 )
  end;
get_size( _Table, 0 )->
  -1.

%%=================================================================
%%	COPY
%%=================================================================
fold(#source{ref = Ref}, Iterator, Acc0)->
  eleveldb:fold(Ref,Iterator, Acc0, [{first_key, first}]).

write_batch(Batch, CopyRef)->
  eleveldb:write(CopyRef,Batch, [{sync, true}]).

drop_batch(Batch0,#source{ref = Ref})->
  Batch =
    [case R of {put,K,_}->{delete,K};_-> R end || R <- Batch0],
  eleveldb:write(Ref,Batch, [{sync, false}]).

action({K,V})->
  {{put,K,V},size(K)+size(V)}.

live_action({write, {K,V}})->
  K1 = ?ENCODE_KEY(K),
  {K1, {put, K1,?ENCODE_VALUE(V)} };
live_action({delete,K})->
  K1 = ?ENCODE_KEY(K),
  {K1,{delete,K1}}.

get_key({put,K,_V})->K;
get_key({delete,K})->K.

decode_key(K)->?DECODE_KEY(K).

rollback_copy( Target )->
  todo.





