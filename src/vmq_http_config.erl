-module(vmq_http_config).
-export([config/0]).


config() ->
    Configs = vmq_config:get_env(http_configs),
    config(Configs, []).

config([{ConfigMod, ConfigFun}|Rest], Routes) ->
    ModRoutes = apply(ConfigMod, ConfigFun, []),
    config(Rest, Routes ++ ModRoutes);
config([], Routes) ->
    Routes.

