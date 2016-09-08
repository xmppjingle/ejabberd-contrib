%%%----------------------------------------------------------------------
%%% File    : mod_grafite.erl
%%% Author  : Thiago Rocha Camargo
%%% Purpose : Gathers statistics and publishes via statsd/grafite
%%% Created :
%%% Id      : $Id: mod_grafite.erl 0000 2016-07-11 16:42:30Z xmppjingle $
%%%----------------------------------------------------------------------

%%%% Definitions

-module(mod_grafite).
-author('rochacamargothiago@gmail.com').
-behaviour(gen_mod).

-include("ejabberd.hrl").
-include("logger.hrl").
-include("jlib.hrl").

-define(HOOKS, [offline_message_hook,
                sm_register_connection_hook, sm_remove_connection_hook,
                user_send_packet, user_receive_packet,
                s2s_send_packet, s2s_receive_packet,
                remove_user, register_user]).

-define(GLOBAL_HOOKS, [component_connected, component_disconnected]).

-export([start/2, stop/1, mod_opt_type/1,
   depends/2, udp_loop_start/1, period_loop/1, push/2]).

-export([offline_message_hook/3,
         sm_register_connection_hook/3, sm_remove_connection_hook/3,
         user_send_packet/4, user_receive_packet/5,
         s2s_send_packet/3, s2s_receive_packet/3,
         remove_user/2, register_user/2, component_connected/1,
         component_disconnected/1, periodic_metrics/1, list_route_count/0,
         list_user_count/0]).

-record(state, {socket, host, port, server}).

-define(PROCNAME, ejabberd_mod_grafite).
-define(PROCNAME_PERIOD, ejabberd_mod_grafite_period).

-define(GRAFITE_KEY(Node, Host, Probe), "mod_grafite.ejabberd." ++ 
  erlang:binary_to_list(Node) ++ "_" ++ 
  erlang:binary_to_list(Host) ++ "." ++ 
  erlang:atom_to_list(Probe)).

%%====================================================================
%% API
%%====================================================================

start(Host, Opts) ->
  [ejabberd_hooks:add(Hook, Host, ?MODULE, Hook, 20)
   || Hook <- ?HOOKS],
  [ejabberd_hooks:add(Hook, ?MODULE, Hook, 18)
   || Hook <- ?GLOBAL_HOOKS],
   StatsDH = gen_mod:get_opt(statsdhost, Opts, fun(X) -> X end, "localhost"),
   {ok, StatsDHost} = getaddrs(StatsDH),
   StatsDPort = gen_mod:get_opt(statsdport, Opts, fun(X) -> X end, 8125),
   Args = [#state{host = StatsDHost, port = StatsDPort, server = Host}],
   start_loop(?PROCNAME, ?MODULE, udp_loop_start, Args),
   start_loop(?PROCNAME_PERIOD, ?MODULE, period_loop, Args),
   ?INFO_MSG("mod_grafite Started on [~p].~n", [Host]).

stop(Host) ->
    [ejabberd_hooks:delete(Hook, Host, ?MODULE, Hook, 20)
      || Hook <- ?HOOKS],
           [ejabberd_hooks:delete(Hook, Host, ?MODULE, Hook, 20)
      || Hook <- ?GLOBAL_HOOKS],
    stop_loop(?PROCNAME),
    stop_loop(?PROCNAME_PERIOD).

start_loop(Name, Module, Function, Args) ->
 case whereis(Name) of
    undefined ->
      register(Name, spawn(Module, Function, Args));
    _ -> ok
 end.

stop_loop(Name) ->
  Proc = whereis(Name),
  case erlang:is_pid(Proc) andalso erlang:is_process_alive(Proc) of
    true ->
       Proc ! stop;
    _ -> ok
  end.

depends(_Host, _Opts) ->
    [].

%%====================================================================
%% Hooks handlers
%%====================================================================

offline_message_hook(_From, #jid{lserver=LServer}, _Packet) ->
    push(LServer, offline_message).

sm_register_connection_hook(_SID, #jid{lserver=LServer}, _Info) ->
    push(LServer, sm_register_connection).
sm_remove_connection_hook(_SID, #jid{lserver=LServer}, _Info) ->
    push(LServer, sm_remove_connection).

user_send_packet(Packet, _C2SState, #jid{lserver=LServer}, _To) ->
    push(LServer, user_send_packet),
    Packet.
user_receive_packet(Packet, _C2SState, _JID, _From, #jid{lserver=LServer}) ->
    push(LServer, user_receive_packet),
    Packet.

s2s_send_packet(#jid{lserver=LServer}, _To, _Packet) ->
    push(LServer, s2s_send_packet).
s2s_receive_packet(_From, #jid{lserver=LServer}, _Packet) ->
    push(LServer, s2s_receive_packet).

remove_user(_User, Server) ->
    push(jid:nameprep(Server), remove_user).
register_user(_User, Server) ->
    push(jid:nameprep(Server), register_user).

component_connected(Host) ->
    push(Host, component_connected).

component_disconnected(Host) ->
    push(Host, component_disconnected).

periodic_metrics(Host) ->
    ?INFO_MSG("Sending Periodic Metrics... [~p]~n", [Host]),
    push(Host, {cluster_nodes, length(ejabberd_cluster:get_nodes())}),
    push(Host, {incoming_s2s_number, ejabberd_s2s:incoming_s2s_number()}),
    push(Host, {outgoing_s2s_number, ejabberd_s2s:outgoing_s2s_number()}),
    lists:foreach(fun({H, C}) -> push(H, {routes, C}) end, list_route_count()),
    lists:foreach(fun({H, C}) -> push(H, {resources, C}) end, list_user_count()).

%%====================================================================
%% metrics push handler
%%====================================================================

push(Host, Probe) ->
    Payload = encode_metrics(Host, Probe),
    whereis(?PROCNAME) ! {send, Payload}.

encode_metrics(Host, Probe) ->
    [_, NodeId] = str:tokens(jlib:atom_to_binary(node()), <<"@">>),
    [Node | _] = str:tokens(NodeId, <<".">>),
    Data = case Probe of
    {Key, Val} ->
        encode(gauge, ?GRAFITE_KEY(Node, Host, Key), Val, 1.0);
    Key ->
        encode(gauge, ?GRAFITE_KEY(Node, Host, Key), 1, 1.0)
    end,
    ?INFO_MSG("Stats: ~p ~p ~n", [Data, encode(gauge, Key, 1, undefined)]),
    Data.

%%====================================================================
%% Grafite/StatsD
%%====================================================================

encode(gauge, Key, Value, _SampleRate) ->
    [Key, ":", format_value(Value), "|g"].

format_value(Value) when is_integer(Value) ->
    integer_to_list(Value);
format_value(Value) when is_float(Value) ->
    float_to_list(Value, [{decimals, 2}]).


%%====================================================================
%% UDP Utils
%%====================================================================

udp_loop_start(#state{}=S) ->
  LocalPort = 44444,
  case gen_udp:open(LocalPort) of
    {ok, Socket} ->
      ?INFO_MSG("UDP Stats Socket Open: [~p]~n", [LocalPort]),
      udp_loop(S#state{socket = Socket});
    _ ->
      ?INFO_MSG("Could not start UDP Socket [~p]~n", [LocalPort])
  end.

udp_loop(#state{} = S) ->
  receive 
    {send, Packet} ->        
      send_udp(Packet, S),
      udp_loop(S);
    stop ->
      ?INFO_MSG("Stopping mod_grafite UDP Loop...~n", []),
      ok;
    _ ->
      udp_loop(S)
  after
    10000 ->
      udp_loop(S)            
  end.

period_loop(#state{server = Host} = S) ->
  receive 
    stop ->
      ?INFO_MSG("Stopping mod_grafite periodic_metrics...~n", []),
      ok;
    _ ->
      period_loop(S)
  after
    60000 ->
      spawn(?MODULE, periodic_metrics, [Host]),
      period_loop(S)            
  end.

send_udp(Payload, #state{socket = Socket, host = Host, port = Port} = State) ->
    case gen_udp:send(Socket, Host, Port, Payload) of
      ok ->
        ok;
      _Error -> 
        ?INFO_MSG("UDP Send Failed: [~p] (~p)~n", [State, Payload])
    end.

getaddrs({_, _, _, _} = Address) ->
    {ok, Address};
getaddrs(Hostname) when is_binary(Hostname) ->
    getaddrs(binary_to_list(Hostname));
getaddrs(Hostname) ->
    case inet:getaddrs(Hostname, inet) of
        {ok, Addrs} ->
            {ok, random_element(Addrs)};
        {error, Reason} ->
            ?ERROR_MSG("getaddrs error: ~p~n", [Reason]),
            {error, Reason}
    end.

random_element([Element]) ->
    Element;
random_element([_|_] = List) ->
    T = list_to_tuple(List),
    Index = random(tuple_size(T)),
    element(Index, T).

random(N) ->
    erlang:phash2({self(), timestamp()}, N) + 1.

timestamp() ->
    os:timestamp().

list_route_count() ->
  lists:map(fun(X) -> {X, length(mnesia:dirty_read(route, X))} end, ejabberd_router:dirty_get_all_routes()).

list_user_count() ->
  DD = lists:foldl(fun({N,D,R}, Dict) -> dict:append(<<N/binary, <<"@">>/binary , D/binary>>, R, Dict) end, dict:new(), ejabberd_sm:dirty_get_sessions_list()),
  lists:map(fun(B) -> {B, length(dict:fetch(B, DD))} end, dict:fetch_keys(DD)).

%%====================================================================
%% mod Options
%%====================================================================

mod_opt_type(statsdhost) -> fun(X) -> X end;
mod_opt_type(statsdport) -> fun(X) when is_integer(X) -> X end;
mod_opt_type(_) ->
    [statsdhost, statsdport].
