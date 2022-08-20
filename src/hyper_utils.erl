-module(hyper_utils).

-export([run_of_zeroes/1, run_of_zeroes/2, changeV/2, m/1]).

run_of_zeroes(B) ->
    run_of_zeroes(1, B).

run_of_zeroes(I, B) ->
    case B of
        <<0:I, _/bitstring>> ->
            run_of_zeroes(I + 1, B);
        _ ->
            I
    end.

changeV(List, ChangeP) ->
    case lists:search(fun({_I, V}) -> V =/= 0 end, enumerate(List)) of
        {value, {I, V}} when I =:= 1 -> V + ChangeP;
        {value, {I, _V}} -> hyper_utils:run_of_zeroes(<<(I - 1):ChangeP/integer>>);
        false -> 0
    end.

enumerate(List) ->
    {List1, _} = lists:mapfoldl(fun(T, Acc) -> {{Acc, T}, Acc + 1} end, 1, List),
    List1.

m(P) ->
    trunc(math:pow(2, P)).
