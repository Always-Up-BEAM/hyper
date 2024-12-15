%% @doc `hyper' is the reference implementation of hyperloglog for erlang
%%
%% Hyperloglog is an algorithm based on a probabilistic datastructure to solve
%% the count-distinct problem. It allows an approximation of the distinct
%% element of a large collection, with a limited memory usage and the ability
%% to be distributed and merged losslessly.
%%
%% This module is the interface you use to interact with the hyperloglog. `hyper'
%% accept multiple backend for the implementation of hyperloglog, each with a
%% different set of tradeoff and performance.
%%
%% For more details on how to use it, look at {@link new/3}, {@link insert/2}
%% and {@link card/1}
%%
%% == How does it work ==
%%
%% Without entering in all the details, the hyperloglog datastructure is based
%% on a limited set of elements. Details of how these are implemented will vary
%% between different backend and implementations, but the high level idea stay
%% the same.
%% - A hash function that output a 64bit hash
%% - A set of numbered "bins", controlled by the precision
%% - An estimator function
%%
%% For every element to insert, the element is hashed into a binary N, then
%% the leftmost P bits are used to define which bin the element belong. In this
%% bin, we store the number of zeroes before the first 1 in the leftmost bit of
%% the leftover, after truncating the leftmost N bit. If this number is bigger
%% than the one currently in the bin, we update it. Otherwise we do nothing. Our
%% input is inserted.
%%
%% Later, when we want to get an estimation of the cardinality, we apply the
%% estimator function to the set of bins. The details of the estimator are too
%% complex for this documentation, but suffice to say that they are proven to
%% reduce the estimation error to stay under a tight boundary.
%%
%% == References ==
%% For more details, here is a list of reference papers that have been used to
%% implement this library.
%%
%% <ul>
%% <li><a href="http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en//pubs/archive/40671.pdf">HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm</a></li>
%% <li><a href="https://arxiv.org/abs/1706.07290"> New Cardinality Estimation Methods for HyperLogLog Sketches</a></li>
%%</ul>
%%  In particular huge thanks to Omar Ertl for his clear work on this domain.
-module(hyper).

-export([new/1, new/2, new/3, insert/2, insert_many/2]).
-export([union/1, union/2]).
-export([card/1, intersect_card/2]).
-export([to_json/1, from_json/1, from_json/2, precision/1, bytes/1, is_hyper/1]).
-export([compact/1, reduce_precision/2]).
-export([generate_unique/1]).

-type precision() :: 4..16.
-type registers() :: any().
-type version() :: atom().

-record(hyper, {v :: version(), registers :: {module(), registers()}}).

-type value() :: binary().
-type filter() :: #hyper{}.

-export_type([filter/0, precision/0, registers/0]).

-define(DEFAULT_BACKEND, hyper_binary).
-define(DEFAULT_VERSION, 'sha-v1').

% constant for 0.5/ln(2)
-define(HLL_ALPHA_INF, 0.721347520444481703680).

%%
%% API
%%

%% @doc Create a new Hyperloglog structure
%% Equivalent to `hyper:new(P, hyper_binary, 'sha-v1')'
%%
%% see {@link hyper:new/3}
-spec new(precision()) -> filter().
new(P) ->
    new(P, ?DEFAULT_BACKEND, ?DEFAULT_VERSION).

%% @doc Create a new Hyperloglog structure with the specified backend
%% Equivalent to `hyper:new(P, Mod, 'sha-v1')'
%%
%% see {@link hyper:new/3}
-spec new(precision(), module()) -> filter().
new(P, Mod) ->
    new(P, Mod, ?DEFAULT_VERSION).

%% @doc Create a new Hyperloglog structure with the specified backend and the
%% specified version.
%%
%% `hyper' ship with one backend:
%%  - {@link hyper_binary}
%%
%% For implementing your own backend, see {@link hyper_register}
%%
%% %% hyper structure with different version will work transparently for now,
%% but will generate deprecation warning in the future if we change some of the
%% implementation details.
%%
%% You should use {@link new/2} most of the time, so that the versionning is
%% handled by `hyper'. You should only use `new/3' if  you need to do your own
%% versionning for custom backends or for custom versionning problems.
-spec new(precision(), module(), version()) -> filter().
new(P, Mod, Version) when 4 =< P andalso P =< 16 andalso is_atom(Mod) ->
    #hyper{v = Version, registers = {Mod, hyper_register:new(Mod, P)}}.

%% @doc return `true' if the structure passed is a `hyper' structure. `false'
%% otherwise
-spec is_hyper(filter()) -> boolean().
is_hyper(#hyper{}) ->
    true;
is_hyper(_) ->
    false.

%% @doc Insert `Value' inside the Hyper structure passed.
-spec insert(value(), filter()) -> filter().
insert(Value, #hyper{registers = {Mod, Registers}} = Hyper) when
    is_binary(Value)
->
    P = hyper_register:precision(Mod, Registers),
    Hash = crypto:hash(sha, Value),
    <<Index:P, RegisterValue:(64 - P)/bitstring, _/bitstring>> = Hash,

    %% Registers are only allowed to increase, implement by backend
    Hyper#hyper{registers = {Mod, hyper_register:set(Mod, Index, RegisterValue, Registers)}};
insert(_Value, _Hyper) ->
    error(badarg).

%% @doc insert a list of values in the Hyper structure.
-spec insert_many([value()], filter()) -> filter().
insert_many(L, Hyper) ->
    lists:foldl(fun insert/2, Hyper, L).

%% Merge a list of hyperloglog together
%% see {@link union/2} for details and implications.
-spec union([filter()]) -> filter().
union(Filters) when is_list(Filters) ->
    case
        lists:usort(
            lists:map(
                fun(#hyper{registers = {Mod, Registers}}) ->
                    {hyper_register:precision(Mod, Registers), Mod}
                end,
                Filters
            )
        )
    of
        %% same P and backend
        [{_P, Mod}] ->
            Registers = lists:map(
                fun(#hyper{registers = {_, R}}) ->
                    R
                end,
                Filters
            ),
            [First | _] = Filters,
            First#hyper{registers = {Mod, hyper_register:max_merge(Mod, Registers)}};
        %% mixed P, but still must have same backend
        [{MinP, Mod} | _] ->
            FoldedFilters = lists:map(
                fun(#hyper{registers = {M, _}} = F) when M =:= Mod ->
                    hyper:reduce_precision(MinP, F)
                end,
                Filters
            ),
            union(FoldedFilters)
    end.

%% Merge two `hyper' structure together.
%%
%% The two structure need to use the same backend.
%%
%% The precision does not have to be the same but the resulting structure will
%% have the lowest precision of the two.
%%
%% See {@link reduce_precision/2} for details of the implications
union(Small, Big) ->
    union([Small, Big]).

%% @doc Provide the cardinality of the intersection of two `hyper' structure
%% using the inclusion/exclusion principle. Use with caution as the precision
%% of this methods is fairly limited.
-spec intersect_card(filter(), filter()) -> float().
intersect_card(
    Left = #hyper{registers = {ModLeft, RegLeft}}, Right = #hyper{registers = {ModRight, RegRight}}
) ->
    PLeft = hyper_register:precision(ModLeft, RegLeft),
    PRight = hyper_register:precision(ModRight, RegRight),
    case PLeft =:= PRight of
        true -> max(0.0, card(Left) + card(Right) - card(union(Left, Right)));
        false -> error(not_equal_precision)
    end.

%% @doc Estimate the cardinality of a  `hyper' structure
-spec card(filter()) -> float().
card(#hyper{registers = {Mod, Registers0}}) ->
    P = hyper_register:precision(Mod, Registers0),
    M = trunc(pow(2, P)),
    Qp1 = 65 - P,
    Registers = hyper_register:compact(Mod, Registers0),
    RegisterHisto = hyper_register:register_histogram(Mod, Registers),

    Z = M * tau(M - maps:get(Qp1, RegisterHisto, 0) / M),
    %TODO: drop after Q = 64 - P in histo before folding
    Z1 = lists:foldr(
        fun({_K, V}, Acc) -> (Acc + V) * 0.5 end,
        Z,
        lists:keysort(1, maps:to_list(maps:without([0, Qp1], RegisterHisto)))
    ),
    Zf = Z1 + M * sigma(maps:get(0, RegisterHisto, 0) / M),
    ?HLL_ALPHA_INF * M * M / Zf.

%% @doc return the precision of the `hyper' structure passed.
precision(#hyper{registers = {Mod, Registers}}) ->
    hyper_register:precision(Mod, Registers).

%% @doc return the size in bytes of the `hyper' structure in memory
bytes(#hyper{registers = {Mod, Registers}}) ->
    hyper_register:bytes(Mod, Registers).

%% @doc compact the underlying `hyper' structure.
%% Do not use, considered an implementation detail of the backend.
%% Will be removed in the future.
compact(#hyper{registers = {Mod, Registers}} = Hyper) ->
    Hyper#hyper{registers = {Mod, hyper_register:compact(Mod, Registers)}}.

%% @doc Reduce the precision of the `hyper' structure to P.
%% This is lossless in the sense that the old structure hold all the data needed
%% to fill the reduced precision one.
%% That said, reducing precision does grow the error in the estimator, and
%% there is no way to get it back. Do this knowing you are losing precision.
reduce_precision(P, #hyper{registers = {Mod, Registers}} = Hyper) ->
    OldP = hyper_register:precision(Mod, Registers),
    case {P, OldP} of
        {P1, P2} when P1 =:= P2 ->
            Hyper;
        {P1, P2} when P1 < P2 ->
            Hyper#hyper{registers = {Mod, hyper_register:reduce_precision(Mod, P, Registers)}};
        _ ->
            error(new_precision_superior_to_old)
    end.

%%
%% SERIALIZATION
%%

%% @deprecated 0.6.0
%% @doc Serialize the `hyper' structure to a json friendly format that can be
%% decoded with {@link from_json/2}
%%
%% Do not use, this is going to be replaced with better solution in the future
-spec to_json(filter()) -> any().
to_json(#hyper{v = V, registers = {Mod, Registers}}) ->
    P = hyper_register:precision(Mod, Registers),
    Compact = hyper_register:compact(Mod, Registers),
    {[
        {<<"p">>, P},
        {<<"v">>, atom_to_binary(V)},
        {<<"registers">>, base64:encode(zlib:gzip(hyper_register:encode_registers(Mod, Compact)))}
    ]}.

%% @deprecated 0.6.0
%% @doc Deserialize the json friendly format from {@link to_json/1} to a `hyper'
%% structure
%%
%% Equivalent to `from_json(Struct, hyper_binary)'
%%
%% Do not use, this is going to be replaced with better solution in the future
-spec from_json(any()) -> filter().
from_json(Struct) ->
    from_json(Struct, ?DEFAULT_BACKEND).

%% @deprecated 0.6.0
%% @doc Deserialize the json friendly format from {@link to_json/1} to a `hyper'
%% structure, with `Mod' as backend
%%
%% Do not use, this is going to be replaced with better solution in the future
-spec from_json(any(), module()) -> filter().
from_json({Struct}, Mod) when is_list(Struct) ->
    P = proplists:get_value(<<"p">>, Struct),
    V = binary_to_atom(proplists:get_value(<<"v">>, Struct)),
    Bytes = zlib:gunzip(base64:decode(proplists:get_value(<<"registers">>, Struct))),
    Registers = hyper_register:decode_registers(Mod, Bytes, P),

    #hyper{v = V, registers = {Mod, Registers}}.

%%
%% HELPERS
%%

generate_unique(N) ->
    generate_unique(lists:usort(random_bytes(N)), N).

generate_unique(L, N) ->
    case length(L) of
        N ->
            L;
        Less ->
            generate_unique(lists:usort(random_bytes(N - Less) ++ L), N)
    end.

random_bytes(N) ->
    random_bytes([], N).

random_bytes(Acc, 0) ->
    Acc;
random_bytes(Acc, N) ->
    Int = rand:uniform(100000000000000),
    random_bytes([<<Int:64/integer>> | Acc], N - 1).

sigma(1.0) ->
    infinity;
sigma(X) ->
    sigma_sum(X, first, X, 1.0).

sigma_sum(Z, Z, _X, _Y) ->
    Z;
sigma_sum(Z, _Zp, X, Y) ->
    X1 = X * X,
    Z1 = (X1 * Y) + Z,
    sigma_sum(Z1, Z, X1, Y + Y).

tau(+0.0) ->
    0.0;
tau(1.0) ->
    0.0;
tau(X) ->
    tau_sum((1 - X), first, X, 1.0) / 3.

tau_sum(Z, Z, _X, _Y) ->
    Z;
tau_sum(Z, _Zp, X, Y) ->
    X1 = math:sqrt(X),
    Y1 = Y * 0.5,
    Z1 = Z - (math:pow(1 - X1, 2) * Y1),
    tau_sum(Z1, Z, X1, Y1).

pow(X, Y) ->
    math:pow(X, Y).
