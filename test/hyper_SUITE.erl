-module(hyper_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

% copy of #hyper in hyper.erl
-record(hyper, {v, registers}).

all() ->
    [
        is_hyper_t,
        basic_t,
        serialization_t,
        backend_t,
        encoding_t,
        register_sum_t,
        error_range_t,
        reduce_precision_t,
        many_union_t,
        union_t,
        union_mixed_precision_t,
        intersect_card_t,
        bad_serialization_t
    ].

is_hyper_t(_Config) ->
    ?assert(hyper:is_hyper(hyper:new(4, hyper_binary))),
    ?assertNot(hyper:is_hyper(foo)).

basic_t(_Config) ->
    [
        ?assertEqual(1, trunc(hyper:card(hyper:insert(<<"1">>, hyper:new(4, Mod)))))
     || Mod <- backends()
    ].

serialization_t(_Config) ->
    Mod = hyper_binary,
    Hyper = hyper:compact(hyper:insert_many(generate_unique(10), hyper:new(5, Mod))),
    HyperJson = (hyper:from_json(hyper:to_json(Hyper), Mod)),

    ?assertEqual(
        trunc(hyper:card(Hyper)),
        trunc(hyper:card(HyperJson))
    ),
    {Mod, Regs} = Hyper#hyper.registers,
    P = hyper_register:precision(Mod, Regs),
    {Mod, RegsJson} = HyperJson#hyper.registers,
    PJson = hyper_register:precision(Mod, RegsJson),
    ?assertEqual(P, PJson).

reduce_precision_t(_Config) ->
    rand:seed(exsss, {1, 2, 3}),
    Card = 10_000,
    Values = generate_unique(Card),
    [
        begin
            HighRes = hyper:insert_many(Values, hyper:new(16, Mod)),
            lists:foreach(
                fun(P) ->
                    Estimate = hyper:card(hyper:reduce_precision(P, HighRes)),
                    M = trunc(math:pow(2, P)),
                    Error = 1.04 / math:sqrt(M),
                    ?assert(abs(Estimate - Card) < Card * Error)
                end,
                lists:seq(10, 15)
            )
        end
     || Mod <- backends()
    ].

backend_t(_Config) ->
    Values = generate_unique(10000),
    P = 9,
    M = trunc(math:pow(2, P)),

    Binary = hyper:compact(hyper:insert_many(Values, hyper:new(P, hyper_binary))),

    {hyper_binary, BinaryRegisters} = Binary#hyper.registers,

    ExpectedRegisters = lists:foldl(
        fun(Value, Registers) ->
            Hash = crypto:hash(sha, Value),
            <<Index:P, RegisterValue:(64 - P)/bitstring, _/bitstring>> =
                Hash,
            ZeroCount = hyper_utils:run_of_zeroes(RegisterValue),
            case Registers of
                #{Index := R} when R > ZeroCount ->
                    Registers;
                _ ->
                    Registers#{Index => ZeroCount}
            end
        end,
        #{},
        Values
    ),
    ExpectedBytes = iolist_to_binary([
        begin
            case ExpectedRegisters of
                #{I := V} ->
                    <<V:8/integer>>;
                _ ->
                    <<0>>
            end
        end
     || I <- lists:seq(0, M - 1)
    ]),
    ?assertEqual(ExpectedBytes, hyper_binary:encode_registers(BinaryRegisters)),

    ?assertEqual(Binary, hyper:from_json(hyper:to_json(Binary), hyper_binary)).

encoding_t(_Config) ->
    [
        begin
            P = 15,
            M = trunc(math:pow(2, P)),
            Hyper = hyper:insert_many(generate_unique(1000), hyper:new(P, Mod)),
            ?assertEqual(
                trunc(hyper:card(Hyper)),
                trunc(hyper:card(hyper:from_json(hyper:to_json(Hyper), Mod)))
            ),
            {Struct} = hyper:to_json(Hyper),
            Serialized = zlib:gunzip(base64:decode(proplists:get_value(<<"registers">>, Struct))),
            WithPadding = <<Serialized/binary, 0>>,

            B = Mod:encode_registers(Mod:decode_registers(Serialized, P)),
            BWithPadding = Mod:encode_registers(Mod:decode_registers(WithPadding, P)),
            ?assertEqual(M, byte_size(B)),
            ?assertEqual(M, byte_size(BWithPadding)),
            ?assertEqual(B, BWithPadding)
        end
     || Mod <- backends()
    ].

register_sum_t(_Config) ->
    Mods = backends(),
    P = 4,
    M = trunc(math:pow(2, P)),

    SetRegisters = [1, 5, 10],
    RegisterValue = 3,
    RegisterInsertValue = <<0:(RegisterValue - 1), 1:1, 0:(61 - P)>>,

    ExpectedSum =
        math:pow(2, -0) * M - math:pow(2, -0) * length(SetRegisters) +
            math:pow(2, -RegisterValue) * length(SetRegisters),
    [
        begin
            Registers = lists:foldl(
                fun(I, Acc) ->
                    Mod:set(I, RegisterInsertValue, Acc)
                end,
                Mod:new(P),
                SetRegisters
            ),
            Compact = Mod:compact(Registers),
            ?assertEqual({Mod, ExpectedSum}, {Mod, Mod:register_sum(Compact)})
        end
     || Mod <- Mods
    ].

error_range_t(_Config) ->
    Mods = backends(),
    Run = fun(Cardinality, P, Mod) ->
        lists:foldl(
            fun(V, H) ->
                hyper:insert(V, H)
            end,
            hyper:new(P, Mod),
            generate_unique(Cardinality)
        )
    end,
    ExpectedError = 0.02,
    P = 14,
    rand:seed(exsss, {1, 2, 3}),

    [
        begin
            Estimate = trunc(hyper:card(Run(Card, P, Mod))),
            ?assert(abs(Estimate - Card) < Card * ExpectedError)
        end
     || Card <- lists:seq(1_000, 50_000, 5_000), Mod <- Mods
    ].

many_union_t(_Config) ->
    rand:seed(exsss, {1, 2, 3}),
    Card = 100,
    NumSets = 3,

    Input = generate_unique(Card),
    [
        begin
            M = trunc(math:pow(2, P)),
            Error = 1.04 / math:sqrt(M),

            Sets = [
                sets:from_list(Input, [
                    {version, 2}
                ])
             || _ <- lists:seq(1, NumSets)
            ],
            Filters = [hyper:insert_many(sets:to_list(S), hyper:new(P, Mod)) || S <- Sets],
            ExpectedFilter = hyper:compact(
                hyper:insert_many(
                    lists:flatten([
                        sets:to_list(S)
                     || S <- Sets
                    ]),
                    hyper:new(P, Mod)
                )
            ),
            H = hyper:union(Filters),

            {Mod, ExpectedRegisters} = ExpectedFilter#hyper.registers,
            {Mod, ActualRegisters} = H#hyper.registers,

            ?assertEqual(
                Mod:encode_registers(ExpectedRegisters),
                Mod:encode_registers(ActualRegisters)
            ),
            Delta = abs(sets:size(sets:union(Sets)) - hyper:card(hyper:union(Filters))),
            case Delta < Card * NumSets * Error of
                true ->
                    ok;
                false ->
                    error_logger:info_msg(
                        "too high error, expected ~.2f%, actual ~.2f%~n~p, p = ~p, card "
                        "= ~p",
                        [Error, Delta / (Card * NumSets), Mod, P, Card]
                    ),
                    ?assert(false)
            end
        end
     || Mod <- backends(), P <- [15]
    ].

union_t(_Config) ->
    rand:seed(exsss, {1, 2, 3}),
    Mod = hyper_binary,

    LeftDistinct = sets:from_list(generate_unique(100)),

    RightDistinct = sets:from_list(
        generate_unique(50) ++
            lists:sublist(sets:to_list(LeftDistinct), 50)
    ),
    LeftHyper = hyper:insert_many(sets:to_list(LeftDistinct), hyper:new(13, Mod)),
    RightHyper = hyper:insert_many(sets:to_list(RightDistinct), hyper:new(13, Mod)),
    UnionHyper = hyper:union([LeftHyper, RightHyper]),
    Intersection = hyper:card(LeftHyper) + hyper:card(RightHyper) - hyper:card(UnionHyper),
    ?assert(
        abs(hyper:card(UnionHyper) - sets:size(sets:union(LeftDistinct, RightDistinct))) <
            200
    ),
    ?assert(
        abs(Intersection - sets:size(sets:intersection(LeftDistinct, RightDistinct))) <
            200
    ).

union_mixed_precision_t(_Config) ->
    [
        ?assertEqual(
            4,
            trunc(
                hyper:card(
                    hyper:union([
                        hyper:insert(<<"1">>, hyper:new(4, Mod)),
                        hyper:insert(<<"2">>, hyper:new(6, Mod)),
                        hyper:insert(<<"3">>, hyper:new(8, Mod)),
                        hyper:insert(<<"4">>, hyper:new(16, Mod))
                    ])
                )
            )
        )
     || Mod <- backends()
    ].

intersect_card_t(_Config) ->
    rand:seed(exsss, {1, 2, 3}),

    LeftDistinct = sets:from_list(generate_unique(10000)),

    RightDistinct = sets:from_list(
        generate_unique(5000) ++
            lists:sublist(sets:to_list(LeftDistinct), 5000)
    ),
    LeftHyper = hyper:insert_many(sets:to_list(LeftDistinct), hyper:new(13)),
    RightHyper = hyper:insert_many(sets:to_list(RightDistinct), hyper:new(13)),

    IntersectCard = hyper:intersect_card(LeftHyper, RightHyper),

    ?assert(IntersectCard =< hyper:card(hyper:union(LeftHyper, RightHyper))),

    %% NOTE: we can't really say much about the error here,
    %% so just pick something and see if the intersection makes sense
    Error = 0.05,
    ?assert(abs(5000 - IntersectCard) / 5000 =< Error).

bad_serialization_t(Config) ->
    [
        begin
            P = 15,
            M = trunc(math:pow(2, P)),
            Data_dir = ?config(data_dir, Config),
            {ok, WithNewlines} = file:read_file(Data_dir ++ "/filter.txt"),
            Raw =
                case zlib:gunzip(base64:decode(binary:replace(WithNewlines, <<"\n">>, <<>>))) of
                    <<RawBytes:M/binary>> ->
                        RawBytes;
                    <<RawBytes:M/binary, 0>> ->
                        %% TODO: test padding
                        RawBytes
                end,
            H = hyper:from_json(
                {[
                    {<<"p">>, 15},
                    {<<"v">>, <<"sha-v1">>},
                    {<<"registers">>, base64:encode(zlib:gzip(Raw))}
                ]},
                Mod
            ),
            {Mod, Registers} = H#hyper.registers,
            Encoded = Mod:encode_registers(Registers),

            ?assertEqual(size(Raw), size(Encoded)),
            lists:foreach(
                fun(I) ->
                    ?assertEqual(binary:at(Raw, I), binary:at(Encoded, I))
                end,
                lists:seq(0, size(Encoded) - 1)
            ),
            ?assertEqual(Raw, Mod:encode_registers(Registers)),

            ?assertEqual(
                {[
                    {<<"p">>, 15},
                    {<<"v">>, <<"sha-v1">>},
                    {<<"registers">>, base64:encode(zlib:gzip(Raw))}
                ]},
                hyper:to_json(H)
            )
        end
     || Mod <- backends()
    ].

%%
%% PROPERTIES
%%

backends() ->
    [
        hyper_binary
    ].

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
    Int = rand:uniform(1 bsl 64),
    random_bytes([<<Int:64/integer>> | Acc], N - 1).

%% Lifted from stdlib2, https://github.com/cannedprimates/stdlib2
partition(N, Xs) when is_integer(N), N > 0, is_list(Xs) ->
    Len = length(Xs),
    case {Len > 0, Len > N} of
        {true, true} ->
            [take(N, Xs) | partition(N, drop(N, Xs))];
        {true, false} ->
            [Xs];
        {false, false} ->
            []
    end.

take(N, _) when N =< 0 ->
    [];
take(_, []) ->
    [];
take(N, [X | Xs]) ->
    [X | take(N - 1, Xs)].

drop(N, Xs) when N =< 0 ->
    Xs;
drop(_, []) ->
    [];
drop(N, [_ | Xs]) ->
    drop(N - 1, Xs).

run_of_zeroes(B) ->
    run_of_zeroes(1, B).

run_of_zeroes(I, B) ->
    case B of
        <<0:I, _/bitstring>> ->
            run_of_zeroes(I + 1, B);
        _ ->
            I
    end.
