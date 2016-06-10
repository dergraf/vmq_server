-module(vmq_subscriber_groups_SUITE).
-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([subscriber_groups_test/1,
         retain_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vmq_commons/include/vmq_types.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(_Config) ->
    %lager:start(),
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,
    lager:info("node name ~p", [node()]),
    _Config.

end_per_suite(_Config) ->
    application:stop(lager),
    _Config.

init_per_testcase(Case, Config) ->
    Nodes = vmq_cluster_test_utils:pmap(
              fun({N, P}) ->
                      Node = vmq_cluster_test_utils:start_node(N, Config, Case),
                      {ok, _} = rpc:call(Node, vmq_server_cmd, listener_start,
                                         [P, []]),
                      {ok, _} = rpc:call(Node, vmq_server_cmd, set_config, [
                                         allow_subscriber_groups, true]),
                      %% allow all
                      ok = rpc:call(Node, vmq_auth, register_hooks, []),
                      {Node, P}
              end, [{test1, 18883},
                    {test2, 18884},
                    {test3, 18885},
                    {test4, 18886},
                    {test5, 18887}]),
    {CoverNodes, _} = lists:unzip(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, _Config) ->
    vmq_cluster_test_utils:pmap(fun(Node) -> ct_slave:stop(Node) end,
                                [test1, test2, test3, test4, test5]),
    ok.

all() ->
    [subscriber_groups_test,
     retain_test].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
subscriber_groups_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    NumSubs = 10,
    NumMsgs = 100,
    _ = start_subscribers(NumSubs, Nodes),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, NumSubs}]),
    Payloads = send(NumMsgs, Nodes, []),
    Ret = mgr_recv_loop(Payloads, #{}),
    check_uniformness(NumSubs, NumMsgs, Ret),
    ?assertEqual([], flush_mailbox([])).

retain_test(Config) ->
    %% we have to test, that all subscribers receive the proper retain
    %% message. Retain handling shouldn't change in case the subscription
    %% is part of a subscriber group.
    ok = ensure_cluster(Config),
    {_, [{_, Port}|_] = Nodes} = lists:keyfind(nodes, 1, Config),
    NumSubs = 10,
    Connect = packet:gen_connect("sub-group-sender", [{clean_session, true},
                                            {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, PubSocket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Topic = "a/b/c",
    Payload = crypto:rand_bytes(64),
    Publish = packet:gen_publish(Topic, 1, Payload, [{mid, 1}, {retain, true}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),
    gen_tcp:close(PubSocket),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, retained, [])
                         end, 1),
    _ = start_subscribers(NumSubs, Nodes),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, NumSubs}]),
    Ret = mgr_recv_loop([Payload||_<-lists:seq(1,NumSubs)], #{}),
    check_uniformness(NumSubs, NumSubs, Ret),
    ?assertEqual([], flush_mailbox([])).

republish_test(Config) ->
    ok = ensure_cluster(Config),
    {_, [{_FirstPort}|_] = Nodes} = lists:keyfind(nodes, 1, Config),
    NumSubs = 10,
    NumMsgs = 100,

    %% Create NumSubs -1 Subscribers on all nodes which are not acking
    %% any PUBLISH they receive.
    NotAckingPids = start_subscribers(NumSubs - 1, Nodes, false, []),
    %% Create 1 Subscriber on one node which is behaving normally
    Self = self(),
    ClientId = "sub-group-client-xyz",
    AckingPid = spawn_link(fun() -> recv_connect(Self, ClientId, Port, true) end),

    %% Wait until cluster has converged
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, NumSubs}]),
    %% Send messages, only one subscriber is properly acking the messages
    Payloads = send(NumMsgs, Nodes, []),
    %% Let's kill one after the other
    Ret = mgr_recv_loop(Payloads, #{}),
    check_uniformness(NumSubs, NumMsgs, Ret),
    ?assertEqual([], flush_mailbox([])).


start_subscribers(N, Nodes) ->
    start_subscribers(N, Nodes, true, []).

start_subscribers(0, _, DoAck, Pids) -> Pids;
start_subscribers(N, [{_, Port} = Node|Nodes], DoAck, Acc) ->
    Self = self(),
    ClientId = "sub-group-client-"++integer_to_list(N),
    Pid = spawn_link(fun() -> recv_connect(Self, ClientId, Port, DoAck) end),
    start_subscribers(N - 1, Nodes ++ [Node], DoAck, [Pid|Acc]).

recv_connect(Parent, ClientId, Port, DoAck) ->
    Connect = packet:gen_connect(ClientId, [{clean_session, true},
                                            {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    SubscriberGroupTopic = "$GROUP-mygroup/a/b/c",
    Subscribe = packet:gen_subscribe(123, SubscriberGroupTopic, 1),
    Suback = packet:gen_suback(123, 1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    inet:setopts(Socket, [{active, true}]),
    recv_loop(Parent, Port, Socket, <<>>, DoAck).

recv_loop(Parent, Port, Socket, Buf, DoAck) ->
    case vmq_parser:parse(Buf) of
        more ->
            ok;
        {error, _} = E ->
            exit(E);
        {#mqtt_publish{message_id=MsgId, payload=Payload}, NewBuf} when DoAck->
            ok = gen_tcp:send(Socket, packet:gen_puback(MsgId)),
            Parent ! {recv, self(), Payload},
            recv_loop(Parent, Port, Socket, NewBuf, DoAck);
        {_, NewBuf} ->
            recv_loop(Parent, Port, Socket, NewBuf, DoAck)
    end,
    receive
        {tcp, Socket, Data} ->
            recv_loop(Parent, Port, Socket, <<Buf/binary, Data/binary>>, DoAck);
        {tcp_closed, Socket} ->
            ok;
        Else ->
            exit(Else)

    end.

flush_mailbox(Acc) ->
    receive
        M ->
            flush_mailbox([M|Acc])
    after
        0 ->
            Acc
    end.


mgr_recv_loop([], Acc) -> Acc;
mgr_recv_loop(Payloads, Acc) ->
    receive
        {recv, Pid, Payload} ->
            Cnt = maps:get(Pid, Acc, 0),
            mgr_recv_loop(Payloads -- [Payload], maps:put(Pid, Cnt + 1, Acc))
    end.

check_uniformness(TotalSubs, TotalMsgs, RecvRet) ->
    ?assertEqual({0,0},
                 maps:fold(fun(_,Cnt, {NumSubs, NumMsgs}) ->
                                   ?assert(Cnt > 0),
                                   {NumSubs - 1,  NumMsgs - Cnt}
                           end, {TotalSubs, TotalMsgs}, RecvRet)).



send(0, _, Acc) -> Acc;
send(N, [{_, Port} = Node|Nodes], Acc) ->
    Connect = packet:gen_connect("sub-group-sender", [{clean_session, true},
                                            {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, PubSocket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    Topic = "a/b/c",
    Payload = crypto:rand_bytes(64),
    Publish = packet:gen_publish(Topic, 1, Payload, [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(PubSocket, "puback", Puback),
    Disconnect = packet:gen_disconnect(),
    gen_tcp:send(PubSocket, Disconnect),
    gen_tcp:close(PubSocket),
    send(N - 1, [Node|Nodes], [Payload|Acc]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ensure_cluster(Config) ->
    [{Node1, _}|OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [begin
         {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, [Node1])
     end || {Node, _} <- OtherNodes],
    {NodeNames, _} = lists:unzip(Nodes),
    Expected = lists:sort(NodeNames),
    ok = vmq_cluster_test_utils:wait_until_joined(NodeNames, Expected),
    [?assertEqual({Node, Expected}, {Node,
                                     lists:sort(vmq_cluster_test_utils:get_cluster_members(Node))})
     || Node <- NodeNames],
    ok.

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {NodeNames, _} = lists:unzip(Nodes),
    vmq_cluster_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_cluster_test_utils:pmap(
                          fun(Node) ->
                                  ExpectedReturn == Fun(Node)
                          end, NodeNames))
      end, 60*2, 500).


