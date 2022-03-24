namespace StringAggregation

module StringAggregation =
    
    open Deedle
    open FSharp.Stats
    open FSharpAux
    open Drafo.Core

    type StringAggregation = 
        | Concat of string 
        
    let initAggregation aggregation  = 
        match aggregation with 
        | Concat sep -> fun (x:seq<string>) -> String.concat sep x       

    /// <summary>
    /// aggregates strings of a column with idendical row keys, after part of the key is droped. Between the strings is a costum string that needs to be added with aggregation
    /// </summary>
    /// <param name="aggregation">has the methode concat and needs costums string</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="col">Column of the frame that should be aggregated</param>
    /// <param name="keyC">seq of string containing the first part of Keys  that shall not be dropped</param>
    /// <param name="filterCols">series of Key,bool that has the same keys as the frame</param>
    let stAggregate aggregation (fp:Frame<Key,_>) (col:string) (keyC:seq<string>) (filterCols:seq<Series<Key,bool>>)=

        let op = initAggregation aggregation
        
        let column = 
            fp
            |>getColumn col
            |> aggregate op dropAllKeyColumnsBut keyC filterCols 
        column

    /// <summary>
    /// aggregates strings of an entire frame, column by column, with idendical row keys, after part of the key is droped. between the strings is a costum string that needs to be added with aggregation
    /// </summary>
    /// <param name="aggregation">has the methode concat and needs costums string</param>
    /// <param name="fp">frame on which the operation is used</param>
    /// <param name="keyC">seq of string containing the first part of Keys for  that shall not be dropped</param>
    /// <param name="filterCols">series of Key,bool that has the same keys as the frame</param>
    let stAggregateFullFrame aggregation (fp:Frame<Key,_>) (keyC:seq<string>) (filterCols:seq<seq<Series<Key,bool>>>)=
        let originalColumnKeys = fp.ColumnKeys

        let colKeysForSeq seqToIt x= 
            seqToIt
            |>Seq.item x

        let forALLColl = 
            originalColumnKeys
            |> Seq.mapi (fun x i -> stAggregate aggregation fp i keyC (colKeysForSeq filterCols x))
            |> Seq.mapi (fun i x-> colKeysForSeq originalColumnKeys i, x:> ISeries<Key>)
            |>assemble
        forALLColl

