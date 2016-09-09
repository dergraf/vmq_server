-module(vmq_mgmt_api).
-export([init/3,
         rest_init/2,
         allowed_methods/2,
         content_types_accepted/2,
         options/2,
%         is_authorized/2,
         malformed_request/2,
         to_json/2]).

-export([routes/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy REST Handler
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(_Transport, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _Opts) ->
    {ok, Req, undefined}.

allowed_methods(Req, State) ->
    {[<<"POST">>, <<"OPTIONS">>, <<"HEAD">>], Req, State}.

content_types_accepted(Req, State) ->
    {[{<<"application/json">>, to_json}], Req, State}.

options(Req0, State) ->
    %% CORS Headers
    Req1 = cowboy_req:set_resp_header(<<"access-control-max-age">>, <<"1728000">>, Req0),
    Req2 = cowboy_req:set_resp_header(<<"access-control-allow-methods">>, <<"HEAD, GET, POST">>, Req1),
    Req3 = cowboy_req:set_resp_header(<<"access-control-allow-headers">>, <<"content-type, authorization">>, Req2),
    Req4 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, <<$*>>, Req3),

    {ok, Req4, State}.

%%is_authorized(Req, State) ->
%%    {ok, Auth, Req1} = cowboy_req:parse_header(<<"authorization">>, Req),
%%    case Auth of
%%        {<<"basic">>, {User, Password}} ->
%%            case validate_account(User, Password) of
%%                ok ->
%%                    {true, Req1, State#state{user=User}};
%%                {error, _Reason} ->
%%                    {{false, <<"Basic realm=\"VerneMQ\"">>}, Req1, State}
%%            end;
%%        _ ->
%%            {{false, <<"Basic realm=\"VerneMQ\"">>}, Req1, State}
%%    end.

malformed_request(Req, State) ->
    {ok, Data, Req1} = cowboy_req:body(Req),
    try validate_json_command(Data) of
        {error, _} ->
            {true, Req1, State};
        M3 ->
            {false, Req1, M3}
    catch
        _:_ ->
            {true, Req1, State}
    end.

to_json(Req, State) ->
    CmdOut = run_command(State),
    case clique_writer:write(CmdOut, "json") of
        {StdOut, []} ->
            Req1 = cowboy_req:set_resp_body(iolist_to_binary(StdOut), Req),
            Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>, Req1),
            {true, Req2, undefined};
        {[], _StdErr} ->
            {ok, Req1} = cowboy_req:reply(400, [], <<"invalid_request_error">>,
                                          Req),
            {false, Req1, State}
    end.

% validate_account(_, _) -> ok.
validate_json_command(JsonCommandObj) ->
    JsonObj = jsx:decode(JsonCommandObj),
    {_, Command} = lists:keyfind(<<"command">>, 1, JsonObj),
    Params = proplists:get_value(<<"params">>, JsonObj, []),
    Flags = proplists:get_value(<<"flags">>, JsonObj, []),
    M0 = clique_command:match(parse_command(Command)
                              ++ parse_params(Params)
                              ++ parse_flags(Flags)
                              ++ ["--format=json"]),
    M1 = clique_parser:parse(M0),
    M2 = clique_parser:extract_global_flags(M1),
    clique_parser:validate(M2).

run_command(M3) ->
    {Res, _, _} = clique_command:run(M3),
    Res.

parse_command(Command) -> ["vmq-admin"] ++ re:split(Command, "/", [{return, list}]).

parse_params(Params) ->
    parse_params(Params, []).

parse_params([], Acc) -> Acc;
parse_params([{Key, true}|Rest], Acc) ->
    parse_params(Rest, [binary_to_list(Key) ++ "=on"|Acc]);
parse_params([{Key, false}|Rest], Acc) ->
    parse_params(Rest, [binary_to_list(Key) ++ "=off"|Acc]);
parse_params([{Key, Val}|Rest], Acc) when is_integer(Val) ->
    parse_params(Rest, [binary_to_list(Key) ++ "=" ++ integer_to_list(Val)|Acc]);
parse_params([{Key, Val}|Rest], Acc) when is_float(Val) ->
    parse_params(Rest, [binary_to_list(Key) ++ "=" ++ float_to_list(Val)|Acc]);
parse_params([{Key, Val}|Rest], Acc) when is_binary(Val) ->
    parse_params(Rest, [binary_to_list(Key) ++ "=" ++ binary_to_list(Val)|Acc]).


parse_flags(Flags) ->
    parse_flags(Flags, []).

parse_flags([], Acc) -> Acc;
parse_flags([{Key, true}|Rest], Acc) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key) ++ "=on"|Acc]);
parse_flags([{Key, false}|Rest], Acc) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key) ++ "=off"|Acc]);
parse_flags([{Key, Val}|Rest], Acc) when is_integer(Val) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key) ++ "=" ++ integer_to_list(Val)|Acc]);
parse_flags([{Key, Val}|Rest], Acc) when is_float(Val) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key) ++ "=" ++ float_to_list(Val)|Acc]);
parse_flags([{Key, Val}|Rest], Acc) when is_binary(Val) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key) ++ "=" ++
                       binary_to_list(Val)|Acc]);
parse_flags([{Key, []}|Rest], Acc) ->
    parse_flags(Rest, ["--" ++ binary_to_list(Key)|Acc]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Cowboy Config
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
routes() ->
    [
     {"/api", ?MODULE, []},
     {"/[...]", cowboy_static, {priv_dir, vmq_server, "ui"}}
    ].
