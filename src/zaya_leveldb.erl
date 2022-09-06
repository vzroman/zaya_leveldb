
-module(zaya_leveldb).

-define(DEFAULT_OPEN_ATTEMPTS, 5).

-define(DEFAULT_ELEVELDB_OPTIONS,#{
  %compression_algorithm => todo,
  open_options=>#{
    create_if_missing => false,
    error_if_exists => false,
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
    sync => false
  }
}).
-define(env(K,D), application:get_env(zaya_leveldb,K,D)).

-define(OPTIONS(O),
  #{
    open_attempts => ?env(open_attempts,?DEFAULT_OPEN_ATTEMPTS),
    eleveldb => ?env(eleveldb,?DEFAULT_ELEVELDB_OPTIONS)
  }
).

-define(EXT,"leveldb").
-define(LOCK(P),P++"/LOCK").

-define(DECODE_KEY(K), sext:decode(K) ).
-define(ENCODE_KEY(K), sext:encode(K) ).
-define(DECODE_VALUE(V), binary_to_term(V) ).
-define(ENCODE_VALUE(V), term_to_binary(V) ).

-define(MOVE(I,K),eleveldb:iterator_move(I,K)).
-define(NEXT(I),eleveldb:iterator_move(I,next)).
-define(PREV(I),eleveldb:iterator_move(I,prev)).

-define(MAX_SEARCH_SIZE,1 bsl 128).

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
  get/2,
  put/2,
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

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( Params ),

  Path = Dir ++"."++?EXT,

  case filelib:is_dir( Path ) of
    false->
      ok;
    true->
      ?LOGERROR("~s directory already exists, try to remove it first",[Path]),
      throw(dir_already_exists)
  end,

  case filelib:is_file( Path ) of
    false->
      ok;
    true->
      ?LOGERROR("~s file already exists, try to remove it first",[Path]),
      throw(dir_is_file)
  end,

  case filelib:ensure_dir( Path ) of
    ok->ok;
    {error, Error}->
      ?LOGERROR("~s unable create error ~p",[Path,Error]),
      throw(unavailable_dir)
  end,

  try_open( Options#{  } ),

  ok.

open( Params)->
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





